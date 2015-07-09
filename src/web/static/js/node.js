var progress_interval;
var node_id = $('#node_id').attr('value');

function progress(jid){
    $.ajax({
        url: '/api/job/' + jid,
        type: 'GET',
        async: false,
        dataType: 'json',
        success: function(data) {
            if (data['status'] == 'pending') {
                $('div.modal-body').html('Pending...');
            }
            if (data['status'] == 'running') {
                $('div.modal-body').html('Running:<br><code>' + data['output'].replace(/\n/g, '<br/>') + '</code>');
            }
            if (data['status'] == 'finished') {
                clearInterval(progress_interval);
                if (data['result']) {
                    $('div.modal-body').html('Finished: Success!');
                    setTimeout(function() {window.location.reload(true);}, 1000);
                } else {
                    $('div.modal-body').html('Finished: Failed<br><code>' + data['output'].replace(/\n/g, '<br/>') + '</code>');
                }
            }
        },
    });
}

$('#node-form-button').click(function() {
    $(this).addClass('disabled');
    $('#node-form-spinner').show();
    // re-enable so serializeArray includes 'disabled' fields
    $('#node-info-form').find(':input:disabled').removeAttr('disabled');
    $.ajax({
        url: '/api/node/' + node_id,
        type: 'POST',
        data: $('#node-info-form').serializeArray(),
        success: function() {
            $('#hostname, #node_id, #FQDN').addClass('disabled');
        },
        error: function() {
            alert('Failed');
            window.location.reload(true);
        },
    })
    .done(function(){
        $('#node-form-spinner').hide();
        $(this).removeClass('disabled');
    });
});

$("#migrate_button").click(function() {
    // re-enable so serializeArray includes 'disabled' fields
    $("#migate_form").find(':input:disabled').removeAttr('disabled');
    $.ajax({
        url: '/api/master/migrate',
        type: 'POST',
        data: $('#migrate_form').serializeArray(),
        dataType: 'json',
        success: function(data) {
            progress_interval = setInterval(function(){progress(data.jid);}, 500);
            $('#migrate_button').remove();
        },
        error: function() {
            alert('Failed');
            window.location.reload(true);
        },
    });
});

$(document).ready(function() {
    $('.app-item').draggable({
        revert: 'invalid',
        stack: '.node-item, .app-item',
        opacity: 0.65,
    });

    $('.node-item').each(function(idx) {
        $(this).droppable({
            accept: '',
            activeClass: 'node-accept',
            tolerance: 'pointer',
            drop: function(event, ui) {
                $('#app_id').attr('value', ui.draggable.attr('app_id'));
                $('#node_id_to').attr('value', $(this).attr('node_id')); 
                $('#migrate_modal').modal('show');
            },
        });
    });

    // Load all master app info
    $('#master-well > .app-item').each(function(_) {
        var app_id = $(this).attr("app_id");
        $.ajax({
            url: '/api/app/' + app_id,
            type: 'GET',
            dataType: 'json',
            success: function(data) {
                $('#app-' + app_id + '-size').html((parseInt(data.maxmemory) / (1024 * 1024)) + 'Mb');
                $('#app-' + app_id + '-cluster').html($(data.nodes).map(function() {
                    if (this.node_id != node_id) {
                        return 'S:' + this.node_id;
                    }
                })
                .get()
                .join(", "));
                
                if(data.nodes.length < 2) {
                    // No slaves, so all nodes are acceptable
                    $('.node-item').each(function(_) {
                        var accept = $(this).droppable("option", "accept");
                        if (accept == "") {
                            $(this).droppable("option", "accept", "#app-" + app_id);
                        } else {
                            $(this).droppable("option", "accept", accept + ", #app-" + app_id);
                        }
                    });
                } else {
                    $(data.nodes).each(function(_) {
                        if (this.role == 'slave') {
                            var node = $('#node-' + this.node_id);
                            var accept = node.droppable("option", "accept");
                            if (accept == "") {
                                node.droppable("option", "accept", "#app-" + app_id);
                            } else {
                                node.droppable("option", "accept", accept + ", #app-" + app_id);
                            }
                        }
                    });
                }
            },
        });
    });

    // Load all slave app info, set UI behaviors
    $('#slave-well > .app-item').each(function(_) {
        var app = $(this);
        var app_id = app.attr("app_id");
        $.ajax({
            url: '/api/app/' + app_id,
            type: 'GET',
            dataType: 'json',
            success: function(data) {
                var master = "";
                $('#app-' + app_id + '-size').html((parseInt(data.maxmemory) / (1024 * 1024)) + 'Mb');
                $('#app-' + app_id + '-cluster').html($(data.nodes).map(function() {
                    if (this.node_id != node_id) {
                        if (this.role == 'master') {
                            return 'M:' + this.node_id;
                        } else {
                            return 'S:' + this.node_id;
                        }
                    }
                })
                .get()
                .sort()
                .join(", "));
                
                // Accept all nodes not in current cluster
                $(data.unused_nodes).each(function(_) {
                    var accept = $('#node-' + this).droppable("option", "accept");
                    if (accept == "") {
                        $('#node-' + this).droppable("option", "accept", "#app-" + app_id);
                    } else {
                        $('#node-' + this).droppable("option", "accept", accept + ", #app-" + app_id);
                    }
                });
            },
        });
    });

    // Load all node info
    $('.node-item').each(function(_) {
        var node_id = $(this).attr("node_id");
        $.ajax({
            url: '/api/node/' + node_id,
            type: 'GET',
            dataType: 'json',
            success: function(data) {
                var perc = 200 * parseFloat(data.memory) / parseFloat(data.total_memory);
                perc = perc.toFixed(2);
                $('#node-' + node_id + '-usage').css('width', perc + '%').html("<span style='text-align: center; color: #000;'>" + perc + "%</span>");
                $('#node-' + node_id + '-masters').html(data.masters.length);
                $('#node-' + node_id + '-slaves').html(data.slaves.length);
            },
        });
    });
});

$('a#node-info-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('#node-info-tab').show();
    $('a#node-info-link').addClass('active');
    $.scrollTo('#node-info-tab', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
});

$('a#allocation-link').click(function() {
    $('ul#side-menu>li>a.active').removeClass('active');
    $('.row-option').hide();
    $('#allocation-tab').show();
    $('a#allocation-link').addClass('active');
    $.scrollTo('#allocation-tab', {
        duration: 1,
        offset: {
            left: 0,
            top: -71,
        },
    });
});
