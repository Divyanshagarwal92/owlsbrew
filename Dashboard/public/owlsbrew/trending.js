

  // WORD CLOUD ///////////////////
      var fill = d3.scale.category20();

      function initWordcloud(words){
          var cloud = d3.layout.cloud()
            cloud
            .size([300, 300])
            .words(words)
              .padding(5)
              .rotate(function() { return ~~(Math.random() * 2) * 90; })
              .font("Impact")
              .fontSize(function(d) { return d.size; })
              .on("end", draw)
              .start();        
      }

      function draw(words) {
        d3.select("#wordcloud").append("svg")
            .attr('class','wordcloud')
            .attr("width", 300)
            .attr("height", 300)
          .append("g")
            .attr("class", "words")
            .attr("transform", "translate(150,150)")
          .selectAll("text")
            .data(words)
          .enter().append("text")
            .attr('opacity', .25)      
            .transition()
            .duration(1000)
            .delay(500)
            .attr('opacity', 1)
            .style("font-size", function(d) { return d.size + "px"; })
            .style("font-family", "Impact")
            .style("fill", function(d, i) { return fill(i); })
            .attr("text-anchor", "middle")
            .attr("transform", function(d) {
              return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
            })
            .text(function(d) { return d.text; });
      }
      function reloadWordcloud(newords){
        d3.selectAll('svg.wordcloud').data([])
          .exit()
          .transition()
          .duration(500)
          .remove();
        var cloud = d3.layout.cloud()
          cloud
          .size([300, 300])
          .words(newords)
            .padding(5)
            .rotate(function() { return ~~(Math.random() * 2) * 90; })
            .font("Impact")
            .fontSize(function(d) { return d.size; })
            .on("end", draw)
            .start();
      }

  words = [
  {text: 'Hello', size: 50},
  {text: 'Word', size: 40},
  {text: 'Small', size: 30},
  {text: 'Smaller', size: 20},
  {text: 'Smallest', size: 10}]

  newords = [
  {text: 'Reload', size: 50},
  {text: 'Word', size: 40},
  {text: 'New', size: 60},
  {text: 'Small', size: 30},
  {text: 'Smaller', size: 20},
  {text: 'Smallest', size: 10}]

  initWordcloud(words);
  reloadWordcloud(newords);
  // END WORD CLOUD
