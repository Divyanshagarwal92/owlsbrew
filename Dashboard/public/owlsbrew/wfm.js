// TODO: data table
// TODO: tooltips
// TODO: legend for choropleth

var usChart = dc.geoChoroplethChart("#map");
var stackedBar = dc.barChart("#bar-chart");
var prodPie = dc.pieChart('#product-pie');
var sizePie = dc.pieChart('#size-pie');
var regionPie = dc.pieChart('#region-pie');
var dataTab = dc.dataTable(".dc-data-table");



function print_filter(filter){
    var f=eval(filter);
    if (typeof(f.length) != "undefined") {}else{}
    if (typeof(f.top) != "undefined") {f=f.top(Infinity);}else{}
    if (typeof(f.dimension) != "undefined") {f=f.dimension(function(d) { return "";}).top(Infinity);}else{}
    console.log(filter+"("+f.length+") = "+JSON.stringify(f).replace("[","[\n\t").replace(/}\,/g,"},\n\t").replace("]","\n]"));
} 

var u, d, s; // for console

queue()
    .defer(d3.csv, '../data/wfm_data_all.csv')
    .defer(d3.json, '/us') //, function(d) { rateById.set(d.id, +d.rate); })
    .await(ready);

function ready(error, data, us){
    d = data; // for console
    u = us; // for console

    for(i in data){
        data[i].date = new Date(data[i].year, data[i].month, data[i].day);
    }

    dataset = crossfilter(data);

var stateDim = dataset.dimension(function(d) {
    return d.geostate;
})

var stateGroup = stateDim.group().reduceSum(function(d) { return d.units });

// convert geo_id to state name
var stateNames = {};
for(i in us.features){
    stateNames[us.features[i].properties.GEO_ID] = us.features[i].properties.NAME
}

// usChart.colorDomain([0,getMax()]);

var numberFormat = d3.format(".f");

function getMax(grp){
    return grp.top(1)[0].value;
}

function getMapColors(){
    color = 'GnBu', numColors = 9;
    mapColors = [], curr = 2;
    while(curr < numColors){
        mapColors.push(colorbrewer[color][numColors][curr++]);
    }
    return mapColors;
}

function updateMap(){
    usChart.width(990)
            .height(500)
            .dimension(stateDim)
            .group(stateGroup)
            .colors(d3.scale.quantize().range(getMapColors()))
            .colorDomain([0, getMax(stateGroup)])
            .colorCalculator(function (d) { return d ? usChart.colors()(d) : '#525252'; })
            .overlayGeoJson(us.features, "state", function (d) {
                return d.properties.GEO_ID;
            })
            .title(function (d) {
                return "State: " + stateNames[d.key] + " Units Sold: " + numberFormat(d.value ? d.value : 0);
            });
}

updateMap();

usChart.on('filtered', function(){ 
    updateMap(); 
});
prodPie.on('filtered', function(){ 
    updateMap(); 
});
regionPie.on('filtered', function(){ 
    updateMap(); 
});
sizePie.on('filtered', function(){ 
    updateMap(); 
});


///////////////////////////////////////////////////

// go for stacked bar... key: month, value: {classic, coco, pink and black}
var dateDim = dataset.dimension(function(d) {
    return d.date;
});

var dateGroup = dateDim.group();

function reduceInitial() {
    return {
        totalUnits: 0,
        totalSales: 0,

        cocoUnits: 0,
        cocoSales: 0,
        
        classicUnits: 0, 
        classicSales: 0,
        
        pinkblackUnits: 0,
        pinkblackSales: 0
    };
}

function reduceAdd(p, v) {
    if (v.product_name === 'The Classic') {
        p.classicUnits += Number(v.units);
        p.classicSales += Number(v.units)*Number(v.price);
    } else if (v.product_name === 'Pink & Black') {
        p.pinkblackUnits += Number(v.units);
        p.pinkblackSales += Number(v.units)*Number(v.price);
    } else {
        p.cocoUnits += Number(v.units);
        p.cocoSales += Number(v.units)*Number(v.price);
    }

    p.totalUnits += Number(v.units);
    p.totalSales += Number(v.units)*Number(v.price);
    return p;
}

function reduceRemove(p, v) {
    if (v.product_name === 'The Classic') {
        p.classicUnits -= Number(v.units);
        p.classicSales -= Number(v.units)*Number(v.price);
    } else if (v.product_name === 'Pink & Black') {
        p.pinkblackUnits -= Number(v.units);
        p.pinkblackSales -= Number(v.units)*Number(v.price);
    } else {
        p.cocoUnits -= Number(v.units);
        p.cocoSales -= Number(v.units)*Number(v.price);
    }

    p.totalUnits -= Number(v.units);
    p.totalSales -= Number(v.units)*Number(v.price);
    return p;
}

function orderValue(p) {
    return p.totalUnits;
}

var dateSales = dateGroup.reduce(reduceAdd, reduceRemove, reduceInitial);
dateSales.order(orderValue);
    var all = dataset.groupAll();
    dc.dataCount('.dc-data-count')
        .dimension(dataset)
        .group(all)
        .html({
            some:'<strong>%filter-count</strong> selected out of <strong>%total-count</strong> records' +
                ' | <a href=\'javascript:dc.filterAll(); dc.renderAll();\'\'>Reset All</a>',
            all:'All records selected. Please click on the graph to apply filters.'
        });


    // DONUT CHARTS ////////////////////////////////
    // QUALITIES TO BE USED
    function getQualities(grp){
        var colorScheme = colorbrewer.YlGnBu[Math.max(3,grp.all().length)];
        var totUnits = dataset.groupAll()
        .reduceSum(function(d){ return d.units; })
        .value(); 

        var quals = {};
        quals.domains = [];
        quals.colors = [];
        quals.numbers = [];
        quals.percentages = [];
        for(i in grp.all()){
            quals.domains.push(grp.all()[i].key)
            quals.colors.push(colorScheme[i]);
            quals.numbers.push(grp.all()[i].value);
            quals.percentages.push(grp.all()[i].value / totUnits);
        }
        return quals;
    }


    var prodDim = dataset.dimension(function(d) { return d.product_name ; });
    var prodGroup = prodDim.group().reduceSum(function(d) { return d.units; });
    var prodQuals = getQualities(prodGroup);
    var prodColorScale = d3.scale.ordinal()
                    .domain(prodQuals.domains)
                    .range(prodQuals.colors);
    
    var pieHeight = 160, pieWidth = 160, pieRadius = 60, innerRadius = 30;


    prodPie
        .width(pieHeight) // (optional) define chart width, :default = 200
        .height(pieWidth) // (optional) define chart height, :default = 200
        .colors(function(d){ 
            return prodColorScale(d); 
        })
        .radius(pieRadius) // define pie radius
        .dimension(prodDim) // set dimension
        .group(prodGroup) // set group
        .innerRadius(innerRadius)
        /* (optional) by default pie chart will use group.key as its label
         * but you can overwrite it with a closure */
        // .label(function (d) {
        //     var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value();
        //     return d.key + ': ' + numeral(Number(d.value) / totUnits).format('0%');
        .label(function (d) {
            var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value(); 
            if (prodPie.hasFilter() && !prodPie.hasFilter(d.key)) {
                return d.key// + '(0%)';
            }
            if (all.value()) {
                return d.key + ' \r \n ' + numeral(Number(d.value) / totUnits).format('0%');
            }
        });


    var sizeDim = dataset.dimension(function(d) { return d.size ; });
    var sizeGroup = sizeDim.group().reduceSum(function(d) { return d.units; });
    var sizeQuals = getQualities(sizeGroup);
    var sizeColorScale = d3.scale.ordinal()
                    .domain(sizeQuals.domains)
                    .range(sizeQuals.colors);
    sizePie
        .width(pieHeight) // (optional) define chart width, :default = 200
        .height(pieWidth) // (optional) define chart height, :default = 200
        .colors(function(d){ 
            return sizeColorScale(d); 
        })
        .radius(pieRadius) // define pie radius
        .dimension(sizeDim) // set dimension
        .group(sizeGroup) // set group
        .innerRadius(innerRadius)
        /* (optional) by default pie chart will use group.key as its label
         * but you can overwrite it with a closure */
        // .label(function (d) {
        //     var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value();
        //     return d.key + ': ' + numeral(Number(d.value) / totUnits).format('0%');
        .label(function (d) {
            var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value();            
            if (sizePie.hasFilter() && !sizePie.hasFilter(d.key)) {
                return d.key// + '(0%)';
            }
            var label = d.key;
            if (all.value()) {
                return d.key + numeral(Number(d.value) / totUnits).format('0%');
            }
        });        


    var regionDim = dataset.dimension(function(d) { return d.region ; });
    var regionGroup = regionDim.group().reduceSum(function(d) { return d.units; });
    var regionQuals = getQualities(regionGroup);
    var regionColorScale = d3.scale.ordinal()
                    .domain(regionQuals.domains)
                    .range(regionQuals.colors);


    regionPie
        .width(pieHeight) // (optional) define chart width, :default = 200
        .height(pieWidth) // (optional) define chart height, :default = 200
        .colors(function(d){ 
            return regionColorScale(d); 
        })
        .radius(pieRadius) // define pie radius
        .dimension(regionDim) // set dimension
        .group(regionGroup) // set group
        .innerRadius(innerRadius)
        /* (optional) by default pie chart will use group.key as its label
         * but you can overwrite it with a closure */
        // .label(function (d) {
        //     var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value();
        //     return d.key + ': ' + numeral(Number(d.value) / totUnits).format('0%');
        .label(function (d) {
            var totUnits = dataset.groupAll().reduceSum(function(d){ return d.units; }).value();            
            if (regionPie.hasFilter() && !regionPie.hasFilter(d.key)) {
                return d.key// + '\n(0%)';
            }
            var label = d.key;
            if (all.value()) {
                return d.key + numeral(Number(d.value) / totUnits).format('0%');
            }
        });    
        
    var minDate = dateDim.bottom(1)[0].date;
    var maxDate = Date.now();

    // when any bar is clicked, recolor the chart 
    var colorRenderlet = function (_chart) {
        _chart.selectAll("rect.bar")
                .on("click", function (d) {
                    function setAttr(selection, keyName) {
                        selection.style("fill", function (d) {
                            if (d[keyName] == "The Classic") return "#63D3FF";
                            else if (d[keyName] == "Pink & Black") return "#FF548F";
                            else if (d[keyName] == "Coco-Lada") return "#9061C2";
                        });
                    };
                    setAttr(_chart.selectAll("g.stack").selectAll("rect.bar"), "layer")
                    setAttr(_chart.selectAll("g.dc-legend-item").selectAll("rect"), "name")
                });
    };

    stackedBar
            .margins({top: 50, right: 20, left: 50, bottom: 50})
            .width(1000)
            .height(400)
            .colors(function(d){ 
                return prodColorScale(d); 
            })
            .gap(50)
            .dimension(dateDim)
            .group(dateSales, "The Classic")
            .valueAccessor(function (d) {
                return d.value.classicUnits;
            })
            .stack(dateSales, "Pink & Black", function (d) {
                return d.value.pinkblackUnits;
            })
            .stack(dateSales, "Coco-Lada", function (d) {
                return d.value.cocoUnits;
            })
            .x(d3.time.scale().domain([minDate, Date.now()]))
            // .xUnits(d3.time.days)
            .xUnits(function(){ return d3.time.days(minDate, maxDate, 4) })
            // .xUnits(d3.time.weeks(minDate, maxDate, 1))            
            .barPadding(0)
            .outerPadding(0)
            .alwaysUseRounding(true)
            .gap(0)
            .centerBar(false)
            .elasticY(true)
            .brushOn(true)
            .renderlet(colorRenderlet)
            .legend(dc.legend().x(100).y(0).itemHeight(13).gap(5));


    var storeDim = dataset.dimension(function(d) { return d.store ; });
    var storeGroup = storeDim.group().reduceSum(function(d) { return d.units; });

dataTab
    .dimension(storeDim)
    .group(function(d) { return d.units + ' units'; })
    .size(10000)
    .columns([
    {
        label: 'Week',
        format: function (d) {
        return moment(d.year + ' ' + d.month + ' ' + d.day).format('LL');
        }
    },
    {
        label: 'Store',
        format: function (d) {
        return d.store;
        }
    },
    {
        label: 'City, State',
        format: function (d) {
            return d.city + ', ' + d.state;
        }
    }             
    ])
    .sortBy(function (d) {
        return d.units + ' units';
    })
    .order(d3.descending)
    .renderlet(function (table) {
        table.selectAll(".dc-table-group").classed("info", true);
    }); 

    dataTab.render();



dc.renderAll();


} // end data import
