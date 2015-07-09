function successList(data) {
    $.each(data, function(proxy, proxy_data) {
        var apps = ''
        $.each(proxy_data['servers'], function(_, app_id) {
          apps += '<a href="/app/' + Number(app_id) + '">' + Number(app_id) + ' </a>'
        });
        $('tbody#proxy-list').append(
        '<tr>' +
          '<td><a href="/proxy/' + Number(proxy) + '">' + proxy_data['name'] + '</a></td>' +
          '<td><code>' + proxy_data['lb'] + '</code></td>' +
          '<td>' + apps + '</td>' +
        '<tr>'
        );
    });
}

function renderList(argdata){
    $('tbody#proxy-list').html('');
    $('div#listcontent').hide();
    $('div#listloading').show();

    $.ajax({
        type: 'GET',
        url: '/api/proxies',
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
    $('ul#proxy-listmenu>li>a.active').removeClass('active');
    state = {owner: '1'};
    history.pushState(state, "owner", '/proxies' + encode_query(state));
    renderList(state);
    $('a#list-owner').addClass('active');
});

$('a#list-operator').on('click', function(e) {
    $('ul#proxy-listmenu>li>a.active').removeClass('active');
    state = {operator: '1'};
    history.pushState(state, "operator", '/proxies' + encode_query(state));
    renderList(state);
    $('a#list-operator').addClass('active');
});

$('a#list-all').on('click', function(e) {
    $('ul#proxy-listmenu>li>a.active').removeClass('active');
    state = {all: '1'};
    history.pushState(state, "all", '/proxies' + encode_query(state));
    renderList({});
    $('a#list-all').addClass('active');
});

/* callback to handle fake javascript history */
window.onpopstate = function(event) {
    if (!event.state) {
        location.reload();
    }
    $('ul#proxy-listmenu>li>a.active').removeClass('active');
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
        $('ul#proxy-listmenu>li>a.active').removeClass('active');
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
