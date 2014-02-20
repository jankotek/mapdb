$(document).ready(function () {
    $("#benchmark").html("<canvas id='benchmark-canvas'/>");
    var g = new Bluff.Bar('benchmark-canvas', 600);
    $.getJSON("js/benchmarks.json", function (data) {

        g.theme_keynote();
        g.title = 'My Graph';

        for (var key in data) {
            g.data(key, [data[key]["read"], data[key]["write"]]);
            $("#benchmarks-table").append(
                "<tr><td>" + key + "</td>" +
                    "<td>" + data[key]["read"] + "</td>" +
                    "<td>" + data[key]["write"] + "</td></tr>"
            );
        }
        g.labels = {0: 'Read', 1: 'Write'};

        g.draw();

    });
});