var IPPORT_REGEX = '([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):([0-9]{4,5})';
var HOSTPORT_REGEX = '([a-zA-Z0-9-_]*):([0-9]{4,5})';
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
                    $('div.modal-body').html('Finished: success!');
                    setTimeout(function() {window.location.reload(true);}, 3000);
                } else {
                    $('div.modal-body').html('Finished: failed :(<br><code>' + data['output'].replace(/\n/g, '<br/>') + '</code>');
                }
            }
        },
    });
}

$("#create_button").click(function() {
        $.ajax({
            url: '/api/proxy',
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

$('#servers').selectize({
    create: false,
    sortField: 'text',
});
$('#operator').selectize({
    create: true,
    sortField: 'text',
});

$('#extservers').selectize({
    valueField: 'serverport',
    labelField: 'server',
    searchField: ['server', 'port'],
    render: {
        item: function(item, escape) {
            return '<div>' +
                '<span class="server">' + escape(item.server) + '</span>:' +
                '<span class="port">' + escape(item.port) + '</span>' +
            '</div>';
        },
        option: function(item, escape) {
            return '<div>' +
                '<span class="server">' + escape(item.serverport) + '</span>' +
            '</div>';
        },
    },
    create: function(input) {
        $(this).options = [];
        var match = input.match(new RegExp('^' + IPPORT_REGEX + '$', 'i'));
        if (match) {
            return {
                server: match[1],
                port: match[2],
                serverport: input,
            }
        }
        match = input.match(new RegExp('^' + HOSTPORT_REGEX + '$', 'i'));
        if (match) {
            return {
                server: match[1],
                port: match[2],
                serverport: input,
            }
        }
        return false
    },
    createFilter: function(input) {
        if ((new RegExp('^' + IPPORT_REGEX + '$', 'i')).test(input)) {
            return true;
        }
        if ((new RegExp('^' + HOSTPORT_REGEX + '$', 'i')).test(input)) {
            return true;
        }
        return false
    },
});
