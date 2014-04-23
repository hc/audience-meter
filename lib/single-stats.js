var http = require('http'),
    url = require('url'),
    merge = require('./utils').merge;

exports.SingleStatsServer = SingleStatsServer;

function SingleStatsServer(options)
{
    if (!(this instanceof SingleStatsServer)) return new SingleStatsServer(options);

    options = merge
    ({
        audience: null,
        port: 8080
    }, options);

    var server = http.Server();
    server.listen(options.port);
    server.on('request', function(req, res)
    {
        var pathname = url.parse(req.url, true).pathname;
        var path2ns = /^\/([^\/]+)?$/;
        res.writeHead(200, {'Content-Type': 'text/html'});
        if ((pathInfo = pathname.match(path2ns)))
        {
            var key = '@' + pathInfo[1];
            if (key == '@total')
            {
                res.write(options.audience.get_total_audience().toString());
            }
            else
            {
                var data = options.audience.get_audience(key);
                if (data)
                {
                    res.write(JSON.stringify(data));
                }
            }
        }
        res.end();
    });
}
