var fs = require('fs'),
    Ftp = require('ftp'),
    csv = require('fast-csv'),
    request = require('request'),
    xml = require('xml');


var ftp = new Ftp();
ftp.on('ready', function() {

	if (!fs.existsSync('tmp')){	
		fs.mkdir('tmp', (err) => {
			if (err) throw err;
		});
  }
  
  ftp.get('feeds/reviews/order-info.csv', (err, stream) => {
    if (err) throw err;
    stream.pipe(fs.createWriteStream('tmp/order-info.csv'));
    stream.once('close', function() { 
      console.log('order-info.csv retrieved');

      ftp.get('feeds/reviews/order-item-info.csv', (err, stream) => {
        if (err) throw err;
        stream.pipe(fs.createWriteStream('tmp/order-item-info.csv'));
        stream.once('close', function() { 
          console.log('order-item-info.csv retrieved');
          convertOrders();

          ftp.list('feeds/search/', (err, list) => {
            if (err) throw err;
            list.forEach(function(file) {
              if (file.name.startsWith('products-')) {
                ftp.get('feeds/search/'+file.name, (err, stream) => {
                  if (err) console.error('There was an error retrieving '+file.name+'. '+err.message);

                  stream.pipe(fs.createWriteStream('tmp/products.csv'));
                  stream.once('close', function() { 
                    console.log('products.csv retrieved');
                    convertProducts();
                  });
                });
              }
            });
          });

        });
      });

    });
  });

});

ftp.connect({
	host: "feeds.futonland.com",
	port: 21,
	user: "constructorio",
	password: "XFSrd0UFZSzG"
});


/* Reviews */

function convertProducts() {
  convertProduct('tmp/products.csv', function(returnValue) {
    console.log('products.csv converted');
    checkReviews(returnValue);
  });
}

function convertProduct(file, callback) {
  var thisFile = [];

  fs.createReadStream(file)
    .pipe(csv.parse({
      delimiter : '\t', 
      quote : null, 
      discardUnmappedColumns : false, 
      headers : true
    }))
    .transform(function(data){
      transformed = {};

      transformed['productId'] = data['Merchant Product ID'];
      transformed['productName'] = data['Title'];
      transformed['productBrand'] = data['Brand/Manufacturer'];
      transformed['productSku'] = data['Manufacturer Part #'];
      transformed['productMpn'] = data['MPN'];
      transformed['productUpc'] = data['UPC'];
      transformed['productUrl'] = data['Product URL'];

      return transformed;
    })
    .on("data", function(data){
      thisFile.push(data);
    })
    .on("end", function(){
      callback(thisFile);
    });
}

function checkReviews(products) {

  var options = { method: 'GET',
    url: 'https://stamped.io/api/widget/reviews?apiKey=pubkey-gS9y55G786s7hhWu14I5gI8Q0cO7uP&storeUrl=info%40futonland.com&take=0'
  };
  request(options, function (error, response, body) {
    if (error) throw new Error(error);

    var parse = JSON.parse(body);
    compileReviews(products, parse.total);

  });

}

function compileReviews(products, total) {

  var take = 50;
  var pages = Math.ceil(total/take);
  var reviews = [];

  console.log('Found '+total+' product reviews');

  for (i=0; i<pages; i++) {
    loadReviews(i);
  }

  function loadReviews(i) {

    setTimeout(function() {

      var options = { method: 'GET',
        url: 'https://stamped.io/api/widget/reviews?apiKey=pubkey-gS9y55G786s7hhWu14I5gI8Q0cO7uP&storeUrl=info%40futonland.com&take='+take+'&page='+(i+1)
      };
      request(options, function (error, response, body) {
        if (error) throw new Error(error);

        if (response.statusCode=='200') {

          var parse = JSON.parse(body);
          length = parse.data.length;

          console.log('Page '+(i+1)+' out of '+pages+' processed successfully');

          for (j=0; j<length; j++) {
            reviews.push(parse.data[j]);

            if (reviews.length == total) {
              processReviews(products, reviews);
            }

          }
        }
        else {
          console.log('Page '+(i+1)+' out of '+pages+' failed to load - '+response.statusCode);
        }

      });

    }, 200 * i);

  }

}

function processReviews(products, reviews) {
  fs.writeFile('tmp/reviews.csv', JSON.stringify(reviews, null, 4), (err) => {
    if (err) throw err;
  });

  var reviewsXml = [ { feed: [ { _attr: { 'xmlns:vc': 'http://www.w3.org/2007/XMLSchema-versioning', 'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'xsi:noNamespaceSchemaLocation': 'http://www.google.com/shopping/reviews/schema/product/2.2/product_reviews.xsd' } } ] } ];

  reviewsXml[0].feed.push( { version: '2.2' } );
  reviewsXml[0].feed.push( { publisher: [ { name: 'Futonland' }, { favicon: 'https://futonland.com/static/icons/favicon-96x96.png' } ] } );
  reviewsXml[0].feed.push( { reviews: [] } );

  for (i=0; i<reviews.length; i++) {

    for (j=0; j<products.length; j++) {
      if ((products[j].productId == reviews[i].productId) || 
        (products[j].productName.substring(0,40) == reviews[i].productName.substring(0,40))) {

        var product_ids = [];
        if (products[j].productUpc) product_ids.push( { gtins: [ { gtin: products[j].productUpc } ] } );
        if (products[j].productMpn) product_ids.push( { mpns: [ { mpn: products[j].productMpn } ] } );
        if (products[j].productSku) product_ids.push( { skus: [ { sku: products[j].productSku } ] } );
        if (products[j].productBrand) product_ids.push( { brands: [ { brand: products[j].productBrand } ] } );

        var review = [];
        review.push( { review_id: reviews[i].id } );
        review.push( { reviewer: [ { name: reviews[i].author } ] } );
        review.push( { review_timestamp: reviews[i].dateCreated } );
        review.push( { title: reviews[i].reviewTitle.replace(/&quot;/g, '"').replace(/&amp;/g, '&') } );
        review.push( { content: reviews[i].reviewMessage.replace(/&quot;/g, '"').replace(/&amp;/g, '&') } );
        review.push( { review_url: [ { _attr: { type: 'singleton' } }, products[j].productUrl+'#stamped-review-'+reviews[i].id ] } );
        review.push( { ratings: [ { overall: [ { _attr: { min: '1', max: '5' } }, reviews[i].reviewRating ] } ] } );
        review.push( { products: [ { product: [ { product_ids: product_ids }, { product_name: products[j].productName }, { product_url: products[j].productUrl } ] } ] } );

        reviewsXml[0].feed[3].reviews.push( { review: review } );

      }
    }

  }

  //console.log(xml(reviewsXml, true));

  fs.writeFile('tmp/google-reviews.xml', xml(reviewsXml, { declaration: true }), (err) => {
    if (err) throw err;
  });

  ftp.put('tmp/google-reviews.xml', 'feeds/reviews/google-reviews.xml', (err) => {
    if (err) throw err;

    console.log("Reviews file uploaded successfully!");
    ftp.close();

  });

}



/* Surveys */

function convertOrders() {
  convertOrder('tmp/order-info.csv', function(returnValue) {
    console.log('order-info.csv converted');
    orderInfoExport = returnValue;
    convertOrder('tmp/order-item-info.csv', function(returnValue) {
      console.log('order-item-info.csv converted');
      orderItemInfoExport = returnValue;
      createOrders(orderInfoExport, orderItemInfoExport);
    });
  });
}

function convertOrder(file, callback) {
  var thisFile = [];

  fs.createReadStream(file)
    .pipe(csv.parse({
      delimiter : ',', 
      quote : '"', 
      discardUnmappedColumns : false, 
      headers : true
    }))
    .transform(function(data){
      transformed = {};

      if (!data['Product ID']) {
        transformed['email'] = data['Email'];
        transformed['firstName'] = data['Customer First Name'];
        transformed['lastName'] = data['Customer Last Name'];
        transformed['location'] = data['City'] + ", " + data['State'];
        transformed['orderNumber'] = data['Order #'];
        transformed['orderId'] = Number(data['Order #']);
        transformed['orderCurrencyISO'] = "USD";
        transformed['orderTotalPrice'] = Number(data['Grand Total']);
        transformed['orderSource'] = data['Processing Channel'];
        transformed['orderDate'] = data['Creation Date'];
        transformed['dateScheduledSet'] = '';
        transformed['orderStatus'] = data['Order Status'];        
      }
      else {
        data['Product ID'] = data['Inactive'] == 'n' ? Number(data['Product ID']) : data['Redirect Product'];
        transformed['orderId'] = Number(data['Order #']);
        transformed['productId'] = data['Product ID'];
        transformed['productBrand'] = data['Manufacturer'];
        transformed['productTitle'] = data['Product Name'];
        transformed['productImageUrl'] = data['Image URL'] ? "http://futonland.com/" + data['Image URL'] : data['Image URL'];
        transformed['productPrice'] = Number(data['Item Total']);
        transformed['productUrl'] = "http://futonland.com/index/page/product/product_id/" + data['Product ID'] + "/product_name/" + encodeURI(data['Product Name']).replace(/%20/g, "+");
      }

      return transformed;
    })
    .on("data", function(data){
      thisFile.push(data);
    })
    .on("end", function(){
      callback(thisFile);
    });
}

function createOrders(orderInfoExport, orderItemInfoExport) {
  for (i=0; i<orderInfoExport.length; i++) {

    // Check for email address
    if (orderInfoExport[i]['email'].length == 0) {
      console.log(orderInfoExport[i]['orderId'] + " - no email"); 
      orderInfoExport.splice(i,1); 
      i--;
      continue;
    }

    // Check for order status
    if (["Voided","Canceled Order","Work Order"].indexOf(orderInfoExport[i]['orderStatus']) > -1) {
      console.log(orderInfoExport[i]['orderId'] + " - " + orderInfoExport[i]['orderStatus']); 
      orderInfoExport.splice(i,1); 
      i--;
      continue;
    }

    // Remove opted-out and unsatisfied
    // if ([152426,152277,152788].indexOf(orderInfoExport[i]['orderId']) > -1) {
    //   console.log(orderInfoExport[i]['orderId'] + " - opted-out"); 
    //   orderInfoExport.splice(i,1); 
    //   i--;
    //   continue;
    // }

    // Set dateScheduledSet
    var schedule = 30;

    var dateScheduledSet = new Date();
    dateScheduledSet.setDate(dateScheduledSet.getDate() + schedule);
    dateScheduledFormatted = dateScheduledSet.getFullYear() + "-" + 
      ('0' + (dateScheduledSet.getMonth()+1)).slice(-2) + "-" +
      ('0' + dateScheduledSet.getDate()).slice(-2) + " " +
      "16:00:00";
    orderInfoExport[i]['dateScheduledSet'] = dateScheduledFormatted;

    orderInfoExport[i]['itemsList'] = [];
    for (j=0; j<orderItemInfoExport.length; j++) {
      if (orderInfoExport[i]['orderId'] == orderItemInfoExport[j]['orderId']) {

        // Check if product is custom
        if (orderItemInfoExport[j]['productTitle'].includes("Item") ||
          orderItemInfoExport[j]['productTitle'].includes("Service") ||
          orderItemInfoExport[j]['productTitle'].includes("Swatch") ||
          orderItemInfoExport[j]['productTitle'].includes("Assorted") ||
          orderItemInfoExport[j]['productTitle'].includes("Floor Sample")) {
          console.log(orderInfoExport[i]['orderId'] + " - " + orderItemInfoExport[j]['productTitle']); 
          orderItemInfoExport.splice(j,1); 
          j--;
          continue;
        }

        // Check if product is inactive and no redirect was set up
        if (orderItemInfoExport[j]['productId'] == '') {
          console.log(orderInfoExport[i]['orderId'] + " - inactive product"); 
          orderItemInfoExport.splice(j,1); 
          j--;
          continue;
        }

        // Change dateScheduledSet // CabinetBed, Innovations, White Lotus Home, Comfort Pure
        if (orderItemInfoExport[j]['productBrand'].includes("CabinetBed") ||
          orderItemInfoExport[j]['productBrand'].includes("Night & Day Furniture") ||
          orderItemInfoExport[j]['productBrand'].includes("Innovations") ||
          orderItemInfoExport[j]['productBrand'].includes("White Lotus Home") ||
          orderItemInfoExport[j]['productBrand'].includes("Comfort Pure") ||
          orderItemInfoExport[j]['productBrand'].includes("Arason") ||
          orderItemInfoExport[j]['productBrand'].includes("Atlantic Furniture") ||
          orderItemInfoExport[j]['productBrand'].includes("Winmark Traders")) {
            console.log(orderInfoExport[i]['orderId'] + " - long processing time"); 
            var schedule = 60;

            var dateScheduledSet = new Date();
            dateScheduledSet.setDate(dateScheduledSet.getDate() + schedule);
            dateScheduledFormatted = dateScheduledSet.getFullYear() + "-" + 
              ('0' + (dateScheduledSet.getMonth()+1)).slice(-2) + "-" +
              ('0' + dateScheduledSet.getDate()).slice(-2) + " " +
              "16:00:00";
            orderInfoExport[i]['dateScheduledSet'] = dateScheduledFormatted;
        }

        orderInfoExport[i]['itemsList'].push(
          { 
            productId: orderItemInfoExport[j]['productId'],
            productBrand: orderItemInfoExport[j]['productBrand'],
            productTitle: orderItemInfoExport[j]['productTitle'],
            productImageUrl: orderItemInfoExport[j]['productImageUrl'],
            productPrice: orderItemInfoExport[j]['productPrice'],
            productUrl: orderItemInfoExport[j]['productUrl']
          }
        );
      }
    }

    // Check if there are any active products left
    if (orderInfoExport[i]['itemsList'].length < 1) {
      console.log(orderInfoExport[i]['orderId'] + " - no active products"); 
      orderInfoExport.splice(i,1); 
      i--;
      continue;
    }

    //delete orderInfoExport[i]['dateScheduledSet'];
    delete orderInfoExport[i]['orderStatus'];

    //console.log(orderInfoExport[i]['itemsList']);
  }
  console.log("Orders array created. Trying to push... " + orderInfoExport.length + " orders");
  //console.log(util.inspect(orderInfoExport, false, null));

  postBulk(orderInfoExport);
}



function postBulk(orders) {
  var options = { method: 'POST',
    url: 'https://stamped.io/api/info@futonland.com/survey/reviews/bulk',
    headers: 
     { authorization: 'Basic cHVia2V5LWdTOXk1NUc3ODZzN2hoV3UxNEk1Z0k4UTBjTzd1UDprZXktTUJuOWtpNHBQV0RKN1h3NTc0MjIyM2lrNjAwOEZo',
       'content-type': 'application/json' },
    body: orders,
    json: true };

  request(options, function (error, response, body) {
    if (error) throw new Error(error);

    console.log("-----");
    //console.log(util.inspect(body, false, null));
    
    console.log(typeof body == 'undefined' ? "Failed!" : "Success! " + body.length + " orders uploaded");

    setTimeout((function() {
        return process.exit();
    }), 3600000);

  });
}
