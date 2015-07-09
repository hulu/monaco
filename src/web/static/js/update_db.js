var progress_interval;

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

function silent_progress(jid) {
    $.ajax({
        url: '/api/job/' + jid,
        type: 'GET',
        async: false,
        dataType: 'json',
        success: function(data) {
            if (data['status'] == 'finished') {
                clearInterval(progress_interval);
                if (data['result']) {
                    window.location.pathname = '/dbs';
                } else {
                    alert('Failed');
                }
            }
        },
    });
}

$("#update_button").click(function() {
    // re-enable so serializeArray includes 'disabled' fields
    $("#update_form").find(':input:disabled').removeAttr('disabled');
    $.ajax({
        url: '/api/app/' + $('#app_id').attr('value'),
        type: 'POST',
        data: $('#update_form').serializeArray(),
        dataType: 'json',
        success: function(data) {
            progress_interval = setInterval(function(){progress(data.jid);}, 500);
            $('#update_button').remove();
        },
        error: function() {
            alert('Failed');
        },
    });
});
$('#delete-link').click(function() {
    if(confirm('Really delete DB? This action is irreversable!')) {
        $.ajax({
            url: '/api/app/' + $('#app_id').attr('value'),
            type: 'DELETE',
            dataType: 'json',
            success: function(data) {
                progress_interval = setInterval(function(){silent_progress(data.jid);}, 500);
                $('#db-metrics').html('');
                $('#node-list-table').hide();
                $('#side-loading').show();
            },
            error: function() {
                alert('Failed');
            },
        });
    }
});

$('#maxmemory').selectize({
    create: false,
    sortField: 'value',
    maxItems: 1,
});

var operselect = $('#operator').selectize({
    create: true,
    sortField: 'text',
});

var ownselect = $('#owner').selectize({
    create: true,
    sortField: 'text',
});
