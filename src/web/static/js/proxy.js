var stat_refresh_interval = 10000;
var stat_refresh_fails = 0;
/* be nice to graphite */
var graph_refresh_interval = 60000;
var graph_refresh_fails = 0;
var interval_id;

$('a#congraph-link').click(function() {
    $.scrollTo('#congraph', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
});
$('a#fegraph-link').click(function() {
    $.scrollTo('#fegraph', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
});
$('a#cegraph-link').click(function() {
    $.scrollTo('#cegraph', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
});

/* reloads graph images */
function refresh_graphs() {
    $("#graphdiv > div > a > img").each(function(idx) {
        var ts = new Date().getTime();
        $(this)
            .attr('src', $(this).attr('src') + '&' + ts)
            .load(function() { graph_refresh_fails = 0; })
            .error(function() {
                graph_refresh_fails = graph_refresh_fails + 1;
                if (graph_refresh_fails > 3) {
                    clearInterval(interval_id);
                    $('#auto-refresh').hide();
                    $('#graph-refresh').show();
                }
            })
        ;
    });
}

/* re-enables graph reloads */
$('#graph-refresh > a').click(function() {
    interval_id = setInterval(refresh_graphs, 10000);
    $('#graph-refresh').hide();
    $('#auto-refresh').show();
});

/* background stats updater, ajax call every 10s */
function refresh_stats() {
    $.ajax({
        url: '/api/proxy/' + $('#twem_id').attr('value') + '/stats',
        dataType: 'json',
        success: function(data) {
            if ('total_rps' in data) {
              $('#rps-target').html(data['total_rps']);
            }
            if ('total_connections' in data) {
              $('#connections-target').html(data['total_connections']);
            }
            stat_refresh_fails = 0;
        },
        error: function() {
            stat_refresh_fails = stat_refresh_fails + 1;
            if (stat_refresh_fails > 3) {
                clearInterval(interval_id);
            }
        },
    });
}
/* define click behavior here so we can setInterval */
$('a#overview-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('.nav-second-level').addClass('collapse');
    $('#overview-submenu').removeClass('collapse');
    $('#overview-page').show();
    $('a#overview-link').addClass('active');
    $.scrollTo('#overview-page', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
    clearInterval(interval_id);
    refresh_stats()
    interval_id = setInterval(refresh_stats, stat_refresh_interval);
});

$('a#metrics-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('.nav-second-level').addClass('collapse');
    $('#metrics-submenu').removeClass('collapse');
    $('#metrics-page').show();
    $('a#metrics-link').addClass('active');
    $.scrollTo('#metrics-page', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
    clearInterval(interval_id);
    refresh_graphs();
    interval_id = setInterval(refresh_graphs, graph_refresh_interval);
});

/* init page */
refresh_stats();
interval_id = setInterval(refresh_stats, stat_refresh_interval);

$.ajax({
    url: '/stats/proxy/' + $('#twem_id').attr('value') + '/client_connections',
    type: 'GET',
    dataType: 'json',
    success: function(result) {
        var container = document.getElementById('congraph');
        var groups = new vis.DataSet();
        for (var i=0; i<result.stat_keys.length; i++) {
            groups.add({
                id: i,
                content: result.stat_keys[i],
                options: {
                    drawPoints: false,
                }
            });
        }
        var dataset = new vis.DataSet();
        for (var i=0l i<result.stat_keys.length; i++) {
            for (var j=0; j<result[i].length; j++) {
                data = {x: result[i][j]['x'], y: result[i][j]['y'], group: i};
                dataset.add(data);
            }
        }
        var options = {
            'dataAxis.customRange.left.min': 0,
            start: result.data['from'],
            end: result.data['to'],
        };
        var Graph2d = new vis.Graph2d(container, dataset, groups, options);
    },
});
$.ajax({
    url: '/stats/proxy/' + $('#twem_id').attr('value') + '/forward_error',
    type: 'GET',
    dataType: 'json',
    success: function(result) {
        var container = document.getElementById('fegraph');
        var groups = new vis.DataSet();
        for (var i=0; i<result.stat_keys.length; i++) {
            groups.add({
                id: i,
                content: result.stat_keys[i],
                options: {
                    drawPoints: false,
                }
            });
        }
        var dataset = new vis.DataSet();
        for (var i=0l i<result.stat_keys.length; i++) {
            for (var j=0; j<result[i].length; j++) {
                data = {x: result[i][j]['x'], y: result[i][j]['y'], group: i};
                dataset.add(data);
            }
        }
        var options = {
            'dataAxis.customRange.left.min': 0,
            start: result.data['from'],
            end: result.data['to'],
        };
        var Graph2d = new vis.Graph2d(container, dataset, groups, options);
    },
});
$.ajax({
    url: '/stats/proxy/' + $('#twem_id').attr('value') + '/client_err',
    type: 'GET',
    dataType: 'json',
    success: function(result) {
        var container = document.getElementById('cegraph');
        var groups = new vis.DataSet();
        for (var i=0; i<result.stat_keys.length; i++) {
            groups.add({
                id: i,
                content: result.stat_keys[i],
                options: {
                    drawPoints: false,
                }
            });
        }
        var dataset = new vis.DataSet();
        for (var i=0l i<result.stat_keys.length; i++) {
            for (var j=0; j<result[i].length; j++) {
                data = {x: result[i][j]['x'], y: result[i][j]['y'], group: i};
                dataset.add(data);
            }
        }
        var options = {
            'dataAxis.customRange.left.min': 0,
            start: result.data['from'],
            end: result.data['to'],
        };
        var Graph2d = new vis.Graph2d(container, dataset, groups, options);
    },
});
