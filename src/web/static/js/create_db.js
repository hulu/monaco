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
                    setTimeout(function() {window.location.reload(true);}, 3000);
                } else {
                    $('div.modal-body').html('Finished: Failed<br><code>' + data['output'].replace(/\n/g, '<br/>') + '</code>');
                }
            }
        },
    });
}

$("#create_button").click(function() {
        // remove 'disabled' class so serializeArray includes them in form
        $('#create_form').find(':input:disabled').removeAttr('disabled');
        $.ajax({
            url: '/api/app',
            type: 'POST',
            data: $('#create_form').serializeArray(),
            dataType: 'json',
            success: function(data) {
                progress_interval = setInterval(function() {progress(data.jid);}, 500);
                $('#create_button').remove();
            },
            error: function() {
                alert('failed!');
            },
        });
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
