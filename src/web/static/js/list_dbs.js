function successList(data) {
    $.each(data, function(app, app_data) {
        $('tbody#monaco-list').append(
        '<tr>' +
          '<td><a href="/app/' + Number(app) + '">' + app_data['service'] + '</a></td>' +
          '<td><code>' + app_data['exposure'] + '</code></td>' +
          '<td class="text-center">' + Math.floor(app_data['memory_percent']) + '%</td>' +
          '<td class="text-center">' + app_data['rps'] + '</td>' +
        '</tr>'
        );
    });
}

function renderList(argdata){
    $('tbody#monaco-list').html('');
    $('div#listcontent').hide();
    $('div#listloading').show();

    $.ajax({
        type: 'GET',
        url: '/api/apps',
        data: argdata,
        dataType: 'json',
        success: successList,
    }).always(function() {
        $('div#listloading').hide();
        $('div#listcontent').show();
    });
}

function encode_query(data) {
    var ret = [];
    for (var d in data) {
        ret.push(encodeURIComponent(d) + '=' + encodeURIComponent(data[d]));
    }
    return '?' + ret.join('&');
}

$('a#list-owner').on('click', function(e) {
    $('ul#db-listmenu>li>a.active').removeClass('active');
    state = {owner: '1'};
    history.pushState(state, "owner", encode_query(state));
    renderList(state);
    $('a#list-owner').addClass('active');
});

$('a#list-operator').on('click', function(e) {
    $('ul#db-listmenu>li>a.active').removeClass('active');
    state = {operator: '1'};
    history.pushState(state, "operator", encode_query(state));
    renderList(state);
    $('a#list-operator').addClass('active');
});

$('a#list-all').on('click', function(e) {
    $('ul#db-listmenu>li>a.active').removeClass('active');
    state = {all: '1'};
    history.pushState(state, "all", encode_query(state));
    renderList({});
    $('a#list-all').addClass('active');
});

/* callback to handle javascript history events */
window.onpopstate = function(event) {
    if (!event.state) {
        return;
    }
    $('ul#db-listmenu>li>a.active').removeClass('active');
    if ('owner' in event.state) {
        renderList(event.state);
        $('a#list-owner').addClass('active');
    } else if('operator' in event.state) {
        renderList(event.state);
        $('a#list-operator').addClass('active');
    } else if('all' in event.state) {
        renderList({});
        $('a#list-all').addClass('active');
    }
}

/* load page from request args for traditional history */
$(document).ready(function(){
    var search = location.search.substring(1);
    var data = {}
    if (search !== '') {
        data = JSON.parse('{"' + decodeURI(search).replace(/"/g, '\\"').replace(/&/g, '","').replace(/=/g,'":"') + '"}');
        $('ul#db-listmenu>li>a.active').removeClass('active');
        if ('owner' in data) {
            $('a#list-owner').addClass('active');
        } else if ('operator' in data) {
            $('a#list-operator').addClass('active');
        } else if ('all' in data) {
            $('a#list-all').addClass('active');
            data = {}
        }
    }
    renderList(data);
});
