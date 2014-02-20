$(document).ready(function () {
    $("#benchmark").html("<canvas id='benchmark-canvas'/>");
    var g = new Bluff.Bar('benchmark-canvas', 600);

    g.theme_keynote();
    g.title = 'My Graph';

    for (var key in basic_data) {
        g.data(key, [basic_data[key]["read"], basic_data[key]["write"]]);
        $("#benchmarks-table").append(
            "<tr><td>" + key + "</td>" +
                "<td>" + basic_data[key]["read"] + "</td>" +
                "<td>" + basic_data[key]["write"] + "</td></tr>"
        );
    }
    g.labels = {0: 'Read', 1: 'Write'};

    g.draw();

});