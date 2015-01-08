// TODO:
// * legend?
// * stacked bar underneath
// * table showing data for what's clicked
var p;
var heatmap = dc.heatMap("#heat", "socialgroup");
var sitePie = dc.pieChart("#sites", "socialgroup");
var dataTab = dc.dataTable(".dc-data-table", "socialgroup");

function print_filter(filter){
    var f=eval(filter);
    if (typeof(f.length) != "undefined") {}else{}
    if (typeof(f.top) != "undefined") {f=f.top(Infinity);}else{}
    if (typeof(f.dimension) != "undefined") {f=f.dimension(function(d) { return "";}).top(Infinity);}else{}
    console.log(filter+"("+f.length+") = "+JSON.stringify(f).replace("[","[\n\t").replace(/}\,/g,"},\n\t").replace("]","\n]"));
} 



var currunit = 'Likes';
var likesDim, likesGroup;
queue()
    // .defer(d3.json, '/mentions')
    .defer(d3.json, '/socialposts') //, function(d) { rateById.set(d.id, +d.rate); })
    .await(ready);

function ready(error, posts){
    p = posts;

    function makeTooltip(d){
        return "<strong>Date:</strong> <span style='color:steelblue'>" + 
        monthTrans[d.key[0]] + ' ' + d.key[1] + "</span></br>" +
        "<strong>" + currunit + "</strong> <span style='color:steelblue'>" + 
        numformat(d.value) + "</span></br>"
    }
    var boxtip = d3.tip()
          .attr('class', 'd3-tip')
          .offset([-10, 0])
          .html(function (d) { return makeTooltip(d);});
  

    var x2014 = [];
    for(i in posts){
        if(posts[i].year === 2014){
            x2014.push(posts[i]);
        }
    }

    var ndx    = crossfilter(x2014);
    likesDim = ndx.dimension(function(d) { return [d.month, d.day]; }),
    likesGroup = likesDim.group().reduceSum(function(d) { return +d.likes; });

    function getMax(grp){
        return grp.top(1)[0].value;
    }

    function getMapColors(){
        color = 'GnBu', numColors = 9;
        mapColors = [], curr = 0;
        while(curr < numColors){
            mapColors.push(colorbrewer[color][numColors][curr++]);
        }
        return mapColors;
    }

    var monthTrans = {
      0: "Jan",
      1: "Feb",
      2: "Mar",
      3: "Apr",
      4: "May",
      5: "Jun",
      6: "Jul",
      7: "Aug",
      8: "Sep",
      9: "Oct",
      10: "Nov",
      11: "Dec"
    };  

    function numformat(val){
        if(currunit == 'Likes/Post'){
            return numeral(val).format('0.0');
        } else { return numeral(val).format('0'); }
    }


    function drawHeatmap(dim, grp){
         // heatmap = dc.heatMap("#heat");
          heatmap
            .width(520)
            .height(520)
            .dimension(dim)
            .group(grp)
            .keyAccessor(function(d) { return d.key[0]; })
            .valueAccessor(function(d) { return d.key[1]; })
            .colorAccessor(function(d) { return +d.value; })
            .colors(d3.scale.quantize().range(getMapColors()))
            .colorDomain([0, getMax(grp)])
            .calculateColorDomain();

          
          heatmap.renderlet(function (chart) {
               chart.selectAll('g.cols').selectAll('text')
                    .text(function(d) { return monthTrans[d]; 
                        })
          });
          heatmap.render();
          d3.selectAll(".heat-box").call(boxtip);
          d3.selectAll(".heat-box")
            .on('mouseover', boxtip.show)
            .on('mouseout', boxtip.hide);                    
    }


    siteDim = ndx.dimension(function(d) { return d.site; }),
    siteGroup = siteDim.group().reduceSum(function(d) { return +d.likes; });
    function drawPie(ndx){
         var prodColorScale = d3.scale.ordinal()
                    .domain(['Instagram', 'Facebook', 'Twitter'])
                    .range([getMapColors()[2],getMapColors()[4],getMapColors()[6]]);

            var pieHeight = 160, pieWidth = 160, pieRadius = 60, innerRadius = 30;
            sitePie
                .width(pieHeight) // (optional) define chart width, :default = 200
                .height(pieWidth) // (optional) define chart height, :default = 200
                .colors(function(d){ 
                    return prodColorScale(d); 
                })
                .radius(pieRadius) // define pie radius
                .dimension(siteDim) // set dimension
                .group(siteGroup) // set group
                .innerRadius(innerRadius)
            sitePie.render();

    }


    drawHeatmap(likesDim, likesGroup);
    drawPie(ndx);

    
    p = x2014;
    
dataTab
    .dimension(likesDim)
    .group(function(d) { return d.site; })
    .size(100)
    .columns([
    {
        label: 'Site',
        format: function (d) {
        return d.site;
        }
    },
    {
        label: 'Date',
        format: function (d) {
            return d.post_date;
        }
    },
    {
        label: 'Likes', // desired format of column name 'Change' when used as a label with a function.
        format: function (d) {
                return d.likes;
        }
    },
    {
        label: 'Comments',
        format: function (d) {
            return d.comments;
        }
    },
    {
        label: 'Text',
        format: function (d) {
            return d.post_text;
        }
    },
    {
        label: 'Type',
        format: function (d) {
            return d.post_type;
        }
    },
    {
        label: 'URL',   
        format: function (d) {
            return d.url;
        }
    }
    ])
    .sortBy(function (d) {
        if (currunit === 'Comments') {
            return d.comments;
        }
        return d.likes;
    })
    .order(d3.descending)
    .renderlet(function (table) {
        table.selectAll(".dc-table-group").classed("info", true);
    }); 

    dataTab.render();
    
    // var insta = [];            
    // for(d in d3.selectAll('td.dc-table-column._6')[0]){
    //     var txt = d3.selectAll('td.dc-table-column._6')[0][d].innerHTML;           
    //     if (txt.indexOf('http://instagram.com') !=-1) {
    //         insta.push(txt);
    //     }
    // }
  
    function updateData(type, filter, currsite, heatFilter, pieFilter, tableFilter){
        ndx = crossfilter(posts);
        if(type === 'site') {
            filter = currunit;
            var x2014 = [];
            if(currsite === 'all'){
                for(i in posts){
                    // posts[i].weekyear = moment(posts[i].post_date).format('MMM YY');
                    if(posts[i].year === 2014){
                        x2014.push(posts[i]);
                    }
                }
            } else {
                    for(i in posts){
                        if(posts[i].year === 2014 && posts[i].site === currsite){
                            x2014.push(posts[i]);
                        }
                    }
            }
            ndx = crossfilter(x2014);
            likesDim = ndx.dimension(function(d) { return [d.month, d.day]; });
        }

        if(filter === 'Likes'){
            likesGroup = likesDim.group().reduceSum(function(d) { return d.likes; });
            siteGroup = siteDim.group().reduceSum(function(d) { return d.likes; });
            // drawHeatmap(likesDim, likesGroup);
        } else if(filter === 'Posts') {
            likesGroup = likesDim.group().reduceSum(function(d) { return 1; });
            siteGroup = siteDim.group().reduceSum(function(d) { return 1; });
            // drawHeatmap(likesDim, likesGroup);
        } else if(filter === 'Comments') {
            likesGroup = likesDim.group().reduceSum(function(d) { return d.comments; });
            siteGroup = siteDim.group().reduceSum(function(d) { return d.comments; });
        } else if(filter === 'Likes + Comments') {
            likesGroup = likesDim.group().reduceSum(function(d) { return d.likes + d.comments; });
            siteGroup = siteDim.group().reduceSum(function(d) { return d.likes + d.comments; });
            // drawHeatmap(likesDim, likesGroup);
        } else if(filter === 'Likes/Post') {
            function reduceInitial() {
                return {
                    posts: 0,
                    likes: 0,
                    likesperpost: 0
                };
            }
            function reduceAdd(p, v) {
                ++p.posts;
                p.likes += v.likes;
                p.likesperpost = p.likes/p.posts;
                return p;
            }
            function reduceRemove(p, v) {
                --p.posts;
                p.likes -= v.likes;
                p.likesperpost = p.likes/p.posts;
                return p;
            }
            function orderValue(p) {
                return p.likesperpost;
            }

            likesGroup = likesDim.group().reduce(reduceAdd, reduceRemove, reduceInitial);
            likesGroup.order(orderValue);
            siteGroup = siteDim.group().reduce(reduceAdd, reduceRemove, reduceInitial);
            siteGroup.order(orderValue);

            for(i in likesGroup.all()){
                likesGroup.all()[i].value = likesGroup.all()[i].value.likesperpost;
            }
            for(i in siteGroup.all()){
                siteGroup.all()[i].value = siteGroup.all()[i].value.likesperpost;
            }

        }

        heatmap = dc.heatMap("#heat", "socialgroup");
          heatmap
            .width(45 * 10 + 80)
            .height(45 * 10 + 5)
            .dimension(likesDim)
            .group(likesGroup)
            .keyAccessor(function(d) { return d.key[0]; })
            .valueAccessor(function(d) { return d.key[1]; })
            .colorAccessor(function(d) { return +d.value; })
            .colors(d3.scale.quantize().range(getMapColors()))
            .colorDomain([0, getMax(likesGroup)])
            .calculateColorDomain();

          
          heatmap.renderlet(function (chart) {
               chart.selectAll('g.cols').selectAll('text')
                    .text(function(d) { return monthTrans[d]; 
              });
          });



        var prodColorScale = d3.scale.ordinal()
                            .domain(['Instagram', 'Facebook', 'Twitter'])
                            .range([getMapColors()[2],getMapColors()[4],getMapColors()[6]]);

        sitePie = dc.pieChart("#sites", "socialgroup");
        var pieHeight = 160, pieWidth = 160, pieRadius = 60, innerRadius = 30;

        sitePie
            .width(pieHeight) // (optional) define chart width, :default = 200
            .height(pieWidth) // (optional) define chart height, :default = 200
            .colors(function(d){ 
                return prodColorScale(d);
            })
            .radius(pieRadius) // define pie radius
            .dimension(siteDim) // set dimension
            .group(siteGroup) // set group
            .innerRadius(innerRadius);

        function tableUnits(likesDim){
            if(currunit === 'Likes'){
                return likesDim.group().reduceSum(function(d) { return d.likes; });
            } else if(currunit === 'Posts') {
                return likesDim.group().reduceSum(function(d) { return 1; });
            } else if(currunit === 'Comments') {
                return likesDim.group().reduceSum(function(d) { return d.comments; });
            } else if(currunit === 'Likes/Post') {
                return likesDim.group().reduce(reduceAdd, reduceRemove, reduceInitial);
                // var lg = likesDim.group().reduce(reduceAdd, reduceRemove, reduceInitial);
                // lg.order(orderValue);
                // for(i in lg.all()){
                //     lg.all()[i].value = lg.all()[i].value.likesperpost;
                // }
                // return lg;
            }

        }

        var tableGroup = tableUnits(likesDim);
        dataTab = dc.dataTable(".dc-data-table", "socialgroup");
        dataTab
            .dimension(likesDim)
            .group(function(d) { return d.site; })
            .size(100)
            .columns([
            {
                label: 'Site',
                format: function (d) {
                return d.site;
                }
            },
            {
                label: 'Date',
                format: function (d) {
                    return d.post_date;
                }
            },
            {
                label: 'Likes', // desired format of column name 'Change' when used as a label with a function.
                format: function (d) {
                        return d.likes;
                }
            },
            {
                label: 'Comments',
                format: function (d) {
                    return d.comments;
                }
            },
            {
                label: 'Text',
                format: function (d) {
                    return d.post_text;
                }
            },
            {
                label: 'Type',
                format: function (d) {
                    return d.post_type;
                }
            },
            {
                label: 'URL',   
                format: function (d) {
                    return d.url;
                }
            }
            ])
            .sortBy(function (d) {
                if (currunit === 'Comments') {
                    return d.comments;
                }
                return d.likes;
            })
            .order(d3.descending)
            .renderlet(function (table) {
                table.selectAll(".dc-table-group").classed("info", true);
            });  


          for(i in heatFilter){
            heatmap.filter([heatFilter[i][0],heatFilter[i][1]])
          }
          for(i in pieFilter){
            sitePie.filter(pieFilter[i]);
          }


        sitePie.render();
        heatmap.render();
        dataTab.render();

        d3.selectAll(".heat-box").call(boxtip);
        d3.selectAll(".heat-box")
            .on('mouseover', boxtip.show)
            .on('mouseout', boxtip.hide); 

        var insta = [];            
        for(d in d3.selectAll('td.dc-table-column._6')[0]){
            var txt = d3.selectAll('td.dc-table-column._6')[0][d].innerHTML;           
            if (txt.indexOf('http://instagram.com') !=-1) {
                insta.push(txt);
            }
        }
        console.log(insta);
        var url = 'http://instagram.com/p/wywqzeSOUx/'
        var embed = '<a href="' + insta[0] + '" style=" color:#000; font-family:Arial,sans-serif; font-size:14px; font-style:normal; font-weight:normal; line-height:17px; text-decoration:none; word-wrap:break-word;" target="_top">'
        var target = document.getElementById('insta1');
        target.innerHTML = embed;
    }

    // heatmap.on('filtered', function(){ 
    //     drawHeatmap(likesDim, likesGroup);
    // });


    // EMBED INSTAGRAM
    // var url = 'http://instagram.com/p/wywqzeSOUx/'
    // var embed = '<a href="' + url + '" style=" color:#000; font-family:Arial,sans-serif; font-size:14px; font-style:normal; font-weight:normal; line-height:17px; text-decoration:none; word-wrap:break-word;" target="_top">'
    // var target = document.getElementById('instagram');
    // target.innerHTML = embed;

    d3.select('#likes').on('click', function(){ 
        currunit = 'Likes';
        if(d3.select('#likes').attr('class') != 'btn bg-green-haze active'){
            d3.select('#likes').attr('class','btn bg-green-haze active');
            d3.select('#posts').attr('class','btn btn-default');
            d3.select('#likesperpost').attr('class','btn btn-default');
            d3.select('#comments').attr('class','btn btn-default');
            d3.select('#likescomments').attr('class','btn btn-default');
            updateData('units', 'Likes', false, heatmap.filters(), sitePie.filters(), dataTab.filters());
        } else { testfilt(); }
    });

    d3.select('#posts').on('click', function(){ 
        currunit = 'Posts';
        if(d3.select('#posts').attr('class') != 'btn bg-green-haze active'){
            d3.select('#posts').attr('class','btn bg-green-haze active');
            d3.select('#likes').attr('class','btn btn-default');
            d3.select('#likesperpost').attr('class','btn btn-default');
            d3.select('#comments').attr('class','btn btn-default');
            d3.select('#likescomments').attr('class','btn btn-default');
            updateData('units', 'Posts', false, heatmap.filters(), sitePie.filters(), dataTab.filters());
        }
    });

    d3.select('#likesperpost').on('click', function(){ 
        currunit = 'Likes/Post';
        if(d3.select('#likesperpost').attr('class') != 'btn bg-green-haze active'){
            d3.select('#likesperpost').attr('class','btn bg-green-haze active');
            d3.select('#posts').attr('class','btn btn-default');
            d3.select('#likes').attr('class','btn btn-default');
            d3.select('#comments').attr('class','btn btn-default');
            d3.select('#likescomments').attr('class','btn btn-default');
            updateData('units', 'Likes/Post', false, heatmap.filters(), sitePie.filters(), dataTab.filters());
        }
    });    
   d3.select('#comments').on('click', function(){ 
        currunit = 'Comments';
        if(d3.select('#comments').attr('class') != 'btn bg-green-haze active'){
            d3.select('#comments').attr('class','btn bg-green-haze active');
            d3.select('#posts').attr('class','btn btn-default');
            d3.select('#likesperpost').attr('class','btn btn-default');
            d3.select('#likes').attr('class','btn btn-default');
            d3.select('#likescomments').attr('class','btn btn-default');
            updateData('units', 'Comments', false, heatmap.filters(), sitePie.filters(), dataTab.filters());
        } else { testfilt(); }
    });
   d3.select('#likescomments').on('click', function(){ 
        currunit = 'Likes + Comments';
        if(d3.select('#likescomments').attr('class') != 'btn bg-green-haze active'){
            d3.select('#likescomments').attr('class','btn bg-green-haze active');
            d3.select('#posts').attr('class','btn btn-default');
            d3.select('#likesperpost').attr('class','btn btn-default');
            d3.select('#likes').attr('class','btn btn-default');
            d3.select('#comments').attr('class','btn btn-default');
            updateData('units', 'Likes + Comments', false, heatmap.filters(), sitePie.filters(), dataTab.filters());
        } else { testfilt(); }
    });
}