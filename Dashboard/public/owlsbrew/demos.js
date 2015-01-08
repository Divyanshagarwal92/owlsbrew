// TODO:
  // incorporate demo+1 data?
  // click bar for region, get state, then store?
  // click bar for month, get day?
  // legend
var duration = 750,
    delay = 25;

var margin = {top: 120, right: 20, bottom: 120, left: 40},
    width = 960 - margin.left - margin.right,
    height = 550 - margin.top - margin.bottom;

var x0 = d3.scale.ordinal()
    .rangeRoundBands([0, width], .1);

var x1 = d3.scale.ordinal();

var y = d3.scale.linear()
    .range([height, 0]);

var color = d3.scale.ordinal()
    .range([colorbrewer.YlGnBu[8][4],colorbrewer.YlGnBu[8][7]]);

var xAxis = d3.svg.axis()
    .scale(x0)
    .orient("bottom");

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .tickFormat(d3.format(".2"));

var svg = d3.select("#bars").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


var monthTrans = {
  0: "Jan '14",
  1: "Feb '14",
  2: "Mar '14",
  3: "Apr '14",
  4: "May '14",
  5: "Jun '14",
  6: "Jul '14",
  7: "Aug '14",
  8: "Sep '14",
  9: "Oct '14",
  10: "Nov '14",
  11: "Dec '14"
};

function tooltip(d){
  var filt;
  if(d.type !== 'Month') {
    filt = d.filter;
  } else {
    filt = monthTrans[d.filter];
  }
  return  "<strong>Before:</strong> <span style='color:steelblue'>" + d.Before + "</span></br>" +
    "<strong>After:</strong> <span style='color:steelblue'>" + d.After + "</span></br>" +
    "<strong>Filter:</strong> <span style='color:steelblue'>" + filt+ "</span></br>";
}

var tip = d3.tip()
  .attr('class', 'd3-tip')
  .offset([-10, 0])
  .html(function(d) {
    return tooltip(d);
  });
svg.call(tip);  

var b;
var prevSize = 0;
d3.json("/demo", function(error, data) {

  var demoNames = ['Before', 'After'];
  function generateData(demoBool, filterType, sort){
    var demosOnly = data.filter(function(x){ return x.demos === demoBool; });
    var dataset = crossfilter(demosOnly);

    // filters: d.region, d.store, d.state, moment(d.date).month();
    var filterOptions = {
      'Month': dataset.dimension(function(d) { return moment(d.date).month(); }),
      'Region': dataset.dimension(function(d) { return d.region; }),
      'Store': dataset.dimension(function(d) { return d.store; }),
      'State': dataset.dimension(function(d) { return d.state; }),
      'Total': crossfilter(data).dimension(function(d) { return d.demos; })
    };


    var dateStore = filterOptions[filterType];
    var beforeStoreGroup = dateStore.group().reduceSum(function(d) { return d[-1] + d[-2] });
    var afterStoreGroup = dateStore.group().reduceSum(function(d) { return d[0] + d[1] });


    // var ageNames = d3.keys(data[0]).filter(function(key) { return key !== "State"; });

    var barData = [];
    beforeStoreGroup.all().forEach(function(bb){
        var found = false;
        afterStoreGroup.all().forEach(function(aa){
            if(found == false){
              if(aa.key == bb.key){
                  units = [{ name: demoNames[0], value: bb.value},
                  { name: demoNames[1], value:  aa.value}];
                  barData.push({
                    'Before': bb.value,
                    'After': aa.value,
                    'filter': bb.key,
                    'type': filterType,
                    'units': units
                  });
                  found = true;
              }
            }
        });
    });
    if(sort === true){
        function compare(a,b) {
          if (a.After < b.After)
             return -1;
          if (a.After > b.After)
            return 1;
          return 0;
        }

        return barData.sort(compare);
    } else { return barData; }
  }

  function demoGrpText(barData, newtext){

    var count = 0, withBefore = 0, 
    withAfter = 0, withoutBefore = 0, withoutAfter = 0;
    withAfter += barData[1].After;
    withBefore += barData[1].Before;
    withoutAfter += barData[0].After;
    withoutBefore += barData[0].Before;

    if(newtext === false){
      svg.append('text')
        .attr('id','with')    
        .attr('x', width*.03)
        .attr('y', -height*.20)
        .text('Increase with demos: ' + numeral((withAfter - withBefore)/withBefore).format('0.0%'))
        .style('font-size', 20);

      svg.append('text')
        .attr('id','without')    
        .attr('x', width*.03)
        .attr('y', -height*.12)
        .text('Increase without demos: ' + numeral((withoutAfter - withoutBefore)/withoutBefore).format('0.0%'))
        .style('font-size', 20); 

      } else { 
        svg.select('#groupsWith')
          .text('')
          .style('font-size', 0);

        svg.select('#groupsWithout')
          .text('')
          .style('font-size', 0);

        svg.select('#with')
          .attr('id','with')    
          .attr('x', width*.03)
          .attr('y', -height*.20)
          .text('Increase with demos: ' + numeral((withAfter - withBefore)/withBefore).format('0.0%'))
          .style('font-size', 20);

        svg.select('#without')
          .attr('id','without')    
          .attr('x', width*.03)
          .attr('y', -height*.12)
          .text('Increase without demos: ' + numeral((withoutAfter - withoutBefore)/withoutBefore).format('0.0%'))
          .style('font-size', 20);         
      }

  }
  
  function typeText(barData){

    function getStats(data){
      var count = 0, grpBefore = 0, grpAfter = 0;
      for(i in data){
        ++count;
        if(data[i].After > data[i].Before){
          ++grpAfter;
        } 
      }
      return [grpAfter/count];
    }


    svg.selectAll('#without')
      .text('')
      .style('font-size', 0);
    svg.selectAll('#with')
      .text('')
      .style('font-size', 0);      

    svg.selectAll('#groupsWith')
      .attr('id','groupsWith')    
      .attr('x', width*.03)
      .attr('y', -height*.20)
      .text('% Groups Showing Increase (Demos): ' + numeral(getStats(barData)).format('0.0%'))
      .style('font-size', 20);

    svg.selectAll('#groupsWithout')
      .attr('id','groupsWithout')    
      .attr('x', width*.03)
      .attr('y', -height*.12)
      .text('% Groups Showing Increase (No Demos): ' + 
          numeral(getStats(generateData(0, barData[0].type))).format('0.0%'))
      .style('font-size', 20);
    
  }

  function drawBars(barData){
    function center(text, pixelSize){
       return width*(.5 - (text.length)*pixelSize/2/width);
    }
    // svg.append('text')
    //   .attr('id','title')
    //   .attr('x', center(titleText, 16))
    //   .attr('y', -height*.20)
    //   .text(titleText)
    //   .style('font-size', '2em');

    // svg.append('text')
    //   .attr('id','subtitle')
    //   .attr('x', center(subtitleText, 11))
    //   .attr('y', -height*.14)
    //   .text(subtitleText)
    //   .style('font-size', '1.5em')
    //   .style('fill', '#636363');
    svg.append('text').attr('id', 'with');
    svg.append('text').attr('id', 'without');
    svg.append('text').attr('id', 'groupsWith');
    svg.append('text').attr('id', 'groupsWithout');
    demoGrpText(barData, false);

    x0.domain(barData.map(function(d) { return d.filter; }));
    x1.domain(demoNames).rangeRoundBands([0, x0.rangeBand()]);


    y.domain([0, d3.max(barData, function(d) { 
      return d3.max(d.units, function(d) { 
        return d.value; 
      }); 
    })]).nice();

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);


    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
      .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Units Sold");

    // DATA //////////////////////////////////////
    var demoStats = svg.selectAll(".demoStats")
        .data(barData)
      .enter().append("g")
        .attr("class", "barGroup")
        .attr("transform", function(d) { return "translate(" + x0(d.filter) + ",0)"; })
        .on('mouseover', tip.show)
        .on('mouseout', tip.hide);

    demoStats.selectAll(".bar")
        .data(function(d) { return d.units; })
      .enter().append("rect")
        .attr("class", "bar")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style("fill", function(d) { return color(d.name); });
    // END DATA //////////////////////////////////

    var legend = svg.selectAll(".legend")
        .data(demoNames.slice().reverse())
      .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

    legend.append("rect")
        .attr("x", width - 18)
        .attr("width", 18)
        .attr("height", 18)
        .style("fill", color);

    legend.append("text")
        .attr("x", width - 24)
        .attr("y", 9)
        .attr("dy", ".35em")
        .style("text-anchor", "end")
        .text(function(d) { return d; });

  }

  function updateBars(barData){
    b = barData;

    if(barData[0].type === 'Total'){
      demoGrpText(barData, true);
    } else {
      typeText(barData);
    }


    // UPDATE AXES
    x0.domain(barData.map(function(d) { return d.filter; }));
    x1.domain(demoNames).rangeRoundBands([0, x0.rangeBand()]);
    y.domain([0, d3.max(barData, function(d) { 
      return d3.max(d.units, function(d) { 
        return d.value; 
      }); 
    })]).nice();

    svg.selectAll(".y.axis").transition()
        .duration(duration)
        .call(yAxis);

    if(barData[0].type !== 'Store'){
      svg.selectAll(".x.axis").transition()
          .duration(duration)
          .call(xAxis);
    } else {
      svg.selectAll(".x.axis").transition()
          .duration(duration)
          .call(xAxis)
          .selectAll('text')
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr('transform', 'rotate(-65)')
            ;            
    }

    if(barData[0].type === 'Month'){
      svg.selectAll(".x.axis")
        .selectAll('text')
        .text(function(d) { return monthTrans[d]; });
    }

    // DATA ///////////////////////////////////
    var groups = svg.selectAll('g .barGroup').data(barData);
    var newGroup = groups.enter().append('g').attr('class','barGroup');
    groups // REMOVE EXCESS GROUPS
        .attr("class", "barGroup")
        .attr("transform", function(d) { return "translate(" + x0(d.filter) + ",0)"; })
        .on('mouseover', tip.show)
        .on('mouseout', tip.hide)
       .exit()
        .transition()
        .duration(duration)
        .style('opacity', 0)
        .remove()
        ;

    var updateBars = groups.selectAll(".bar")
        .data(function(d) { return d.units; });

    updateBars // UPDATES DATA ON EXISTING BARS
        .attr("class", "bar")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style('opacity', 0)
        .transition()
        .duration(duration)
        .attr("class", "bar")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style('opacity', 1)
        .style("fill", function(d) { return color(d.name); });

    updateBars // APPENDS BARS TO G WITH NO BARS
      .enter().append("rect")
        .attr("class", "bar")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style('opacity', 0)
        .style("fill", function(d) { return color(d.name); })
        .transition()
        .duration(duration)
        .attr("class", "bar")
        .attr("width", x1.rangeBand())
        .attr("x", function(d) { return x1(d.name); })
        .attr("y", function(d) { return y(d.value); })
        .attr("height", function(d) { return height - y(d.value); })
        .style('opacity', 1)
        .style("fill", function(d) { return color(d.name); });



    // console.log('AFTER DELETE');
    // console.log(svg.selectAll(".barGroup"));
    // console.log(svg.selectAll(".bar"));
    // console.log(svg.selectAll(".barGroup")[0].length);
    // console.log(svg.selectAll(".bar")[0].length);


    // console.log('AFTER ADD');
    // console.log(svg.selectAll(".barGroup"));
    // console.log(svg.selectAll(".bar"));
    // console.log(svg.selectAll(".barGroup")[0].length);
    // console.log(svg.selectAll(".bar")[0].length);
  

    // // END DATA //////////////////////////////////

    var legend = svg.selectAll(".legend")
        .data(demoNames.slice().reverse())
      .enter().append("g")
        .attr("class", "legend")
        .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

    legend.append("rect")
        .attr("x", width - 18)
        .attr("width", 18)
        .attr("height", 18)
        .style("fill", color);

    legend.append("text")
        .attr("x", width - 24)
        .attr("y", 9)
        .attr("dy", ".35em")
        .style("text-anchor", "end")
        .text(function(d) { return d; });

  }
















  var active;

  $('#region').click(function() {
    active = 'Region';
    if($('#region').attr('class') !== 'btn bg-green-haze active'){
        $('#region').attr('class','btn bg-green-haze active');
        $('#month').attr('class','btn btn-default');
        $('#store').attr('class','btn btn-default');
        $('#state').attr('class','btn btn-default');
        $('#total').attr('class','btn btn-default');
        updateBars(generateData(1, 'Region', false));      
    }
  });

  $('#month').click(function() {
    active = 'Month';
    if($('#month').attr('class') !== 'btn bg-green-haze active'){
        $('#month').attr('class','btn bg-green-haze active');
        $('#region').attr('class','btn btn-default');
        $('#store').attr('class','btn btn-default');
        $('#state').attr('class','btn btn-default');
        $('#total').attr('class','btn btn-default');
       updateBars(generateData(1, 'Month', false));      
    }
  });

  $('#state').click(function() {
    active = 'State';
    if($('#state').attr('class') !== 'btn bg-green-haze active'){
        $('#state').attr('class','btn bg-green-haze active');
        $('#month').attr('class','btn btn-default');
        $('#store').attr('class','btn btn-default');
        $('#region').attr('class','btn btn-default');
        $('#total').attr('class','btn btn-default');
        updateBars(generateData(1, 'State', false));      
    }
  });

  $('#store').click(function() {
    active = 'Store';
    if($('#store').attr('class') !== 'btn bg-green-haze active'){
        $('#store').attr('class','btn bg-green-haze active');
        $('#month').attr('class','btn btn-default');
        $('#region').attr('class','btn btn-default');
        $('#state').attr('class','btn btn-default');
        $('#total').attr('class','btn btn-default');
        updateBars(generateData(1, 'Store', false));      
    }
  }); 

  $('#total').click(function() {
    active = 'Total';
    if($('#total').attr('class') !== 'btn bg-green-haze active'){
        $('#total').attr('class','btn bg-green-haze active');
        $('#month').attr('class','btn btn-default');
        $('#region').attr('class','btn btn-default');
        $('#state').attr('class','btn btn-default');
        $('#store').attr('class','btn btn-default');
        updateBars(generateData(1, 'Total', false));      
    }
  });   

  $('#sort').click(function() {
    console.log('sort?');
    if(active !== 'Total'){
      console.log('yes');
      updateBars(generateData(1, active, true));   
    }
  });

  active = 'Total';
  drawBars(generateData(1, 'Total', false));


});
