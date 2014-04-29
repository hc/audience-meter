var http = require('http'),
    url = require('url'),
    merge = require('./utils').merge;

exports.HadoopStatsServer = HadoopStatsServer;

function HadoopStatsServer(options)
{
    if (!(this instanceof HadoopStatsServer)) return new HadoopStatsServer(options);

    options = merge
    ({
        audience: null,
        port: 8080,
        hostname: '',
        auth_username: 'hadoop_stats',
        auth_password: 'GwKBkEiBXLyqRODTbBWpkhXY5FJvwVk7RthQzasgqTNQlpQDTksYEOnKbejlKBsR'
    }, options);

    var server = http.Server();
    server.listen(options.port);
    server.on('request', function(req, res)
    {
        if (req.method === 'POST')
        {
            var header = req.headers['authorization'] || '';
            var token = header.split(/\s+/).pop();
            var auth = new Buffer(token, 'base64').toString();
            var parts=auth.split(/:/);
            var username = parts[0];
            var password=parts[1];
            if (username == options.auth_username && password == options.auth_password)
            {
                var body = '';
                req.on('data', function (data) {
                    body += data;
                });
                req.on('end', function () {
                    data = JSON.parse(body);
                    options.audience.setHadoopNamespaces(data);
                });
                res.writeHead(200,
                {
                    'Content-Length': '0',
                    'Connection': 'close'
                });
            }
            else
            {
                res.writeHead(403,
                {
                    'Content-Length': '0',
                    'Connection': 'close'
                });
            }
        }
        else
        {
            var pathname = url.parse(req.url, true).pathname;
            var path2ns = /^\/([^\/]+)?$/;
            if ((pathInfo = pathname.match(path2ns)))
            {
                var key = pathInfo[1];
                var data = options.audience.get_audience_from_hadoop(key);
                if (data)
                {
                    res.write(JSON.stringify(data));
                }
                else
                {
                    res.write(JSON.stringify(options.audience.getHadoopNamespaces()));
                }
            }
        }
        res.end();
    });
}
