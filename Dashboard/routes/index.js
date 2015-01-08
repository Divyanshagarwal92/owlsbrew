var express = require('express');
var router = express.Router();
var fs = require('fs');
var moment = require('moment');

var bodyParser = require('body-parser');
var parseUrlencoded = bodyParser.urlencoded({extended : false});

var mysql = require('mysql');
var dbParams = {
      host     : 'twittmap.ct78jelemnjv.us-east-1.rds.amazonaws.com',
      user     : 'ebroot',
      password : 'JoshPriya9'
      ,port        :  3306
    };
var connection = mysql.createConnection(dbParams);
connection.connect(function(err) {
  if (err) {
    console.error('error connecting: ' + err.stack);
    return;
  }
});

function readJSONFile(filename, callback) {
  fs.readFile(filename, function (err, data) {
    if(err) {
      callback(err);
      return;
    }
    try {
      callback(null, JSON.parse(data));
    } catch(exception) {
      callback(exception);
    }
  });
}

router.route('/socialposts')
.get(function(req, res){
	var q = "select 'Twitter' as site, created_at as post_date, NULL as post_time, text as post_text, fav_count as likes, NULL as comments, NULL as description, NULL as url, NULL as post_type from owlsbrew.tweets  where username = 'TheOwlsBrew' \
union \
select 'Instagram' as site, date as post_date, time as post_time, text as post_text, num_likes as likes, num_comments as comments, NULL as description, url as url, NULL as post_type from owlsbrew.media M \
union \
select 'Facebook' as site, created_time as post_date, NULL as post_time, message as post_text, P.no_of_likes as num_likes, count(*) as num_comments, post_description as description, links as url, post_type as post_type from owlsbrew.fb_comments C, owlsbrew.fb_posts P where C.post_id = P.post_id group by created_time, post_description, links, message, P.no_of_likes, post_type \
";
	connection.query(q, function(err, social, fields){
		// TODO: fix dates
	    for(i in social){
	        if(social[i].site === 'Facebook'){
	            social[i].post_date = moment(social[i].post_date).format('LLL');
	        } else if(social[i].site === 'Twitter') {
	            social[i].post_date = moment(social[i].post_date.split(' ')
	            	.slice(0,3).join(' ') + ' ' + 
	            	social[i].post_date.split(' ')[5]  + ' ' + 
	            	social[i].post_date.split(' ')[3]).format('LLL');
	        } else {
	        	social[i].post_date = moment(social[i].post_date + ' ' + social[i].post_time)
	        							.format('LLL');
	        }
	        delete social[i].post_time;
	        social[i]['year'] = moment(social[i].post_date).year();
	        social[i]['month'] = moment(social[i].post_date).month();
	        social[i]['day'] = moment(social[i].post_date).date();
	        social[i]['hour'] = moment(social[i].post_date).hour();	        
	    }

		res.json(social);
	});

});

router.route('/mentions')
.get(function(req, res){
	var q = "select source_type as site, published_at as post_date, description as post_text, NULL as num_likes, tone as polarity, tone_score as polarity_score, title, url, source_name, source_url, picture_url, 'mention' as db_source from owlsbrew.mention where source_name != 'TheOwlsBrew' and source_name != 'BrewLabTea' and (source_type != 'twitter' or published_at < '2014-12-01') \
union \
select 'twitter' as site, created_at as post_date, text as pot_text, fav_count as num_likes, polarity, polarity_score, NULL as title, NULL as url, NULL as source_name, NULL as source_url, NULL as picture_url, 'twitter' as db_source from owlsbrew.tweets where username != 'TheOwlsBrew' and username not like 'Beer%' and text not like '%beer%' \
order by site, post_text";
	connection.query(q, function(err, mentions, fields){
	    for(i in mentions){
	        if(mentions[i].db_source === 'mention'){
	            mentions[i].post_date = moment(mentions[i].post_date).format('LLL');
	        } else {
	            mentions[i].post_date = moment(mentions[i].post_date.split(' ')
	            	.slice(0,3).join(' ') + ' ' + 
	            	mentions[i].post_date.split(' ')[5]  + ' ' + 
	            	mentions[i].post_date.split(' ')[3]).format('LLL');
	        }
	        mentions[i]['year'] = moment(mentions[i].post_date).year();
	        mentions[i]['month'] = moment(mentions[i].post_date).month();
	        mentions[i]['day'] = moment(mentions[i].post_date).date();
	        mentions[i]['hour'] = moment(mentions[i].post_date).hour();
	    }

		res.json(mentions);
	});

});


router.route('/demo')
.get(function(req, res){
	var q = "select year, month, day, units_sold, demos,\
		ST.store, ST.region, ST.state \
		from (select year, month, day, WFM.units as units_sold, 0 as demos, storenum \
		from (select wfm_year, wfm_month, wfm_day, \
			case when units_sold is NULL then 0 else units_sold \
			end as units, wfm as demo, wfm_store from \
		(select wfm_year, wfm_month, wfm_day, wfm_store, \
			wfm from owlsbrew.demos_new where wfm = 1) D LEFT OUTER JOIN\
		(select year, month, day, storenum, count(*) as units_sold from \
			owlsbrew.wfm_sales group by year, month, day, storenum) WFM \
		ON D.wfm_year = WFM.year and D.wfm_month = WFM.month and \
		D.wfm_day = WFM.day and D.wfm_store = WFM.storenum) DEMODAYS \
		RIGHT OUTER JOIN owlsbrew.wfm_sales WFM \
		ON WFM.month = DEMODAYS.wfm_month and WFM.year = DEMODAYS.wfm_year \
		and WFM.day = DEMODAYS.wfm_day \
		   and WFM.storenum = DEMODAYS.wfm_store \
		WHERE demo is null or demo = 0 \
		GROUP BY year, month, day, storenum \
		UNION \
		(select wfm_year, wfm_month, wfm_day, case when units_sold is \
			NULL then 0 else units_sold end as units, wfm as demo, wfm_store from\
		(select wfm_year, wfm_month, wfm_day, wfm_store, wfm from owlsbrew.demos_new\
		 where wfm = 1) D LEFT OUTER JOIN \
		(select year, month, day, storenum, count(*) as units_sold from owlsbrew.wfm_sales \
			group by year, month, day, storenum) WFM \
		ON D.wfm_year = WFM.year and D.wfm_month = WFM.month and D.wfm_day = WFM.day \
		and D.wfm_store = WFM.storenum)) X, owlsbrew.wfm_stores ST\
		where ST.storenum = X.storenum";
	connection.query(q, function(err, demo, fields){
		var demoStats = {}, demoStatsArr;
			for(j in demo){
		    demo[j].date = moment(new Date(demo[j].year, demo[j].month-1, demo[j].day)).format('LL');
		    if (!demoStats[demo[j].date])
		      // if date doesn't exist
		      demoStats[demo[j].date] = {};
		    demoStats[demo[j].date][demo[j].store] = {};

		    demoStats[demo[j].date][demo[j].store]['demos'] = demo[j].demos;
		    demoStats[demo[j].date][demo[j].store]['region'] = demo[j].region;
		    demoStats[demo[j].date][demo[j].store]['state'] = demo[j].state;
		    demoStats[demo[j].date][demo[j].store][0] = demo[j].units_sold;
		    demoStats[demo[j].date][demo[j].store][-1] = 0;
		    demoStats[demo[j].date][demo[j].store][-2] = 0;
		    demoStats[demo[j].date][demo[j].store][1] = 0;
		    demoStats[demo[j].date][demo[j].store]['demo_-1'] = 0;
		    demoStats[demo[j].date][demo[j].store]['demo_-2'] = 0;
		    demoStats[demo[j].date][demo[j].store]['demo_+1'] = 0;
		}

		for(i in demo){
		  // create object with week 0,1,-1,-2 and whether there was a demo
		  if (demoStats[moment(demo[i].date).add(7, 'days').format('LL')]
		   && demoStats[moment(demo[i].date).add(7, 'days').format('LL')][demo[i].store]){
		      demoStats[moment(demo[i].date).add(7, 'days').format('LL')][demo[i].store][-1] = demo[i].units_sold;
		      demoStats[moment(demo[i].date).add(7, 'days').format('LL')][demo[i].store]['demo_-1'] = demo[i].demos;

		   }
		  if (demoStats[moment(demo[i].date).add(14, 'days').format('LL')]
		   && demoStats[moment(demo[i].date).add(14, 'days').format('LL')][demo[i].store]){
		       demoStats[moment(demo[i].date).add(14, 'days').format('LL')][demo[i].store][-2] = demo[i].units_sold;
		       demoStats[moment(demo[i].date).add(14, 'days').format('LL')][demo[i].store]['demo_-2'] = demo[i].demos;
		    }
		  if (demoStats[moment(demo[i].date).subtract(7, 'days').format('LL')]
		   && demoStats[moment(demo[i].date).subtract(7, 'days').format('LL')][demo[i].store]){
		      demoStats[moment(demo[i].date).subtract(7, 'days').format('LL')][demo[i].store][1] = demo[i].units_sold;
		      demoStats[moment(demo[i].date).subtract(7, 'days').format('LL')][demo[i].store]['demo_+1'] = demo[i].demos;
		   }
		}


		// flatten it out to 1 flat list
		demoStatsArr = [];
		Object.keys(demoStats).forEach(function(date){
		Object.keys(demoStats[date]).forEach(function(store){
		  var curr = {};
		  curr['date'] = date;
		  curr['store'] = store;
		  Object.keys(demoStats[date][store]).forEach(function(k){
		    curr[k] = demoStats[date][store][k];
		  });
		  demoStatsArr.push(curr);
		});
		});

		res.json(demoStatsArr);
	});
});

router.route('/us')
.get(function(req, res){
	var path = __dirname + '/../public/data/us.json';
	var data = readJSONFile(path, function(err, json){
			if(err) { throw err; }
			res.json(json);
	});
});

router.route('/us-counties')
.get(function(req, res){
	var path = __dirname + '/../public/data/us-counties.json';
	var data = readJSONFile(path, function(err, json){
			if(err) { throw err; }
			res.json(json);
	});
});


router.route('/dcmaps')
.get(function(req, res){
	var path = __dirname + '/../public/data/demo.tsv';
	console.log(path);
	var data = readJSONFile(path, function(err, json){
			if(err) { throw err; }
			res.json(json);
	});
});

router.route('/districts')
.get(function(req, res){
	var path = __dirname + '/../public/data/wb_rw_districts.json';
	console.log(path);
	var data = readJSONFile(path, function(err, json){
			if(err) { throw err; }
			res.json(json);
	});
});

router.route('/red_alerts')
.get(function(req, res){
	var path = __dirname + '/../public/data/red_alerts.json';
	var data = readJSONFile(path, function(err, json){
			if(err) { throw err; }
			res.json(json);
	});
});


router.route('/yelp')
.get(function(req, res){
	res.json(yelp);
	// var path = __dirname + '/../public/data/yelp.json';
	// var data = readJSONFile(path, function(err, json){
	// 		if(err) { throw err; }
	// 		res.json(json);
	// });
});

module.exports = router;