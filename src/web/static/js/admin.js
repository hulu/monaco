// 

function updateLatency() {
    $('#latency-refresh').addClass('glyphicon-refresh-animate');
    $.ajax({
        url: '/api/master_latency',
        type: 'GET',
        dataType: 'json',
        success: function(data) {
            $('#latency').html('Latency: ' + data.computed_duration.toFixed(2));
        },
        error: function() {
            $('#latency').html('API ERROR!');
        },
    })
    .always(function() {
        $('#latency-refresh').removeClass('glyphicon-refresh-animate');
    });
}

function updateStatus() {
    $('#status-refresh').addClass('glyphicon-refresh-animate');
    $('#status-output').hide();
    $('#status-glyphicon').removeClass();
    $.ajax({
        url: '/api/system_health',
        type: 'GET',
        dataType: 'json',
        success: function(data) {
            if (data.result) {
                $('#short-status').html('OK');
                $('#status-glyphicon').addClass('glyphicon glyphicon-ok');
                $('#status-output').html('');
            } else {
                $('#short-status').html('SYSTEM ERRORS!');
                $('#status-glyphicon').addClass('glyphicon glyphicon-remove');
                $('#status-output').show();
                $('#status-output').html(data.output);
            }
        },
        error: function() {
            $('#short-status').html('system check unavailable');
            $('#status-glyphicon').addClass('glyphicon glyphicon-question-mark');
            $('#status-output').show();
            $('#status-output').html('Unable to query system health');
        },
    })
    .always(function() {
        $('#status-refresh').removeClass('glyphicon-refresh-animate');
    });
}


function renderMonacoNodes() {
    $('#monacotable').hide();
    $('#monacoloading').show();
    $('#monaco-nodes > tbody').html('');

    $.ajax({
        url: '/api/nodes',
        type: 'GET',
        dataType: 'json',
        success: function(data) {
            $('#monacotable > h4').html('Monaco Nodes');
            $('#monacotable > h4').addClass('text-success');

            $.each(data.node_ids, function(_, nid) {
                // List entry in table
                if (data[nid].role === 'master') {
                    $('#monaco-nodes > tbody').append(
                        '<tr data-toggle="collapse" data-target="#' + data[nid].hostname + '" class="accordion-toggle success">' +
                          '<td><a href="/node/' + nid + '">' + nid + '</a></td>' +
                          '<td>' + data[nid].hostname + '</td>' +
                          '<td>' + data[nid].mem_usage + '</td>' +
                          '<td> ... </td>' +
                          '<td> master </td>' +
                          '<td>' + data[nid].role_details.connected_slaves + '</td>' +
                        '</tr>'
                    );
                } else if (data[nid].role === 'slave') {
                    $('#monaco-nodes > tbody').append(
                        '<tr class="info">' +
                          '<td><a href="/node/' + nid + '">' + nid + '</a></td>' +
                          '<td>' + data[nid].hostname + '</td>' +
                          '<td>' + data[nid].mem_usage + '</td>' +
                          '<td> ... </td>' +
                          '<td> slave </td>' +
                          '<td>' + data[nid].role_details.master + '</td>' +
                        '</tr>'
                    );
                } else {
                    $('#monaco-nodes > tbody').append(
                        '<tr class="danger">' + 
                          '<td><a href="/node/' + nid + '">' + nid + '</a></td>' +
                          '<td>' + data[nid].hostname + '</td>' +
                          '<td></td><td></td>' +
                          '<td>Unknown</td>' +
                          '<td></td>' +
                        '</tr>'
                    );
                }
                // Format visible row
                if (data[nid].up !== true) {
                    $('#monaco-nodes > tbody > tr:last').removeClass('success');
                    $('#monaco-nodes > tbody > tr:last').removeClass('info');
                    $('#monaco-nodes > tbody > tr:last').addClass('danger');
                }
                // Collapsed row
                if (data[nid].role === 'master') {
                    var inner_table = ''
                    for(var i=0; i<data[nid].role_details.connected_slaves; i++) {
                        inner_table = inner_table +
                                  '<tr><td rowspan=4> slave' + i + '</td><td>ip</td><td>' + data[nid].role_details['slave' + i].ip + '</td></tr>' +
                                  '<tr><td>lag</td><td>' + data[nid].role_details['slave' + i].lag + '</td></tr>' +
                                  '<tr><td>offset</td><td>' + data[nid].role_details['slave' + i].offset + '</td></tr>' +
                                  '<tr><td>state</td><td>' + data[nid].role_details['slave' + i].state + '</td></tr>';
                    }

                    $('#monaco-nodes > tbody').append(
                        '<tr>' +
                          '<td colspan="6" style="padding:0;">' +
                            '<div class="collapse" id="' + data[nid].hostname + '">' +
                              '<table class="table table-condensed table-extension">' +
                                '<thead><tr><th>Slave</th><th></th><th></th></tr></thead>' +
                                '<tbody>' +
                                  inner_table +
                                '</tbody>' +
                              '</table>' +
                            '</div>' +
                          '</td>' +
                        '</tr>'
                    );
                }
            });
        }
    }).always(function() {
        $('#monacoloading').hide();
        $('#monacotable').show();
    });
}

/* Selectizery */
$('#node_id').selectize({
    valueField: 'value',
    maxItems: 1,
    create: true,
    /* FIXME
     * createFilter: function(query) {
        $.ajax({
            url: '/api/nodes',// + query,
            type: 'GET',
            dataType: 'json',
            success: function(data) {
                console.log(data);
                if(query in data.node_ids) {
                    return false;
                }
            },
        });
        return true;
    },*/
});

$('#total_memory').selectize({
    maxItems: 1,
    create: true,
    createFilter: '[0-9]*',
});

$('#create_button').click(function() {
    var data = $('#create_form').serializeArray();
    $.each(data, function(idx, obj) {
        if (obj.name == 'FQDN') {
            obj.value = $('#hostname').val() + obj.value;
        }
    });
    console.log(data);
    $.ajax({
        url: '/api/nodes', //+ $('#node_id').val(),
        type: 'POST',
        data: data,
        success: function(data) {
            alert('Success!');
            window.location.reload(true);
        },
        error: function(data) {
            alert('Error!');
            window.location.reload(true);
        },
    });
});

$('a#overview-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('.nav-second-level').addClass('collapse');
    $('#overview-submenu').removeClass('collapse');
    $('#overview-page').show();
    $('a#overview-link').addClass('active');
    $.scrollTo('#nodes-page', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
    updateLatency();
    updateStatus();
});

$('a#nodes-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('.nav-second-level').addClass('collapse');
    $('#nodes-submenu').removeClass('collapse');
    $('#nodes-page').show();
    $('a#nodes-link').addClass('active');
    $.scrollTo('#nodes-page', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
    renderMonacoNodes();
});

$(document).ready(function() {
    updateLatency();
    updateStatus();
});
