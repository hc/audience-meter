var http = require('http'),
    url = require('url'),
    fs = require('fs'),
    merge = require('./utils').merge;

exports.DemoServer = DemoServer;

function DemoServer(options)
{
    if (!(this instanceof DemoServer)) return new DemoServer(options);

    options = merge({port: 8080}, options);

    var server = http.Server();
    server.listen(options.port);
    server.on('request', function(req, res)
    {
        var path = url.parse(req.url, true).pathname;

        if (path == '/')
        {
            res.writeHead(200, {'Content-Type': 'text/html'});
            res.end('Welcome to audience-meter. Try <a href="/test">an event</a>.');
        }
        else if(/^[a-z.\/]+\.(js|swf)$/.test(path))
        {
            if (/\.js$/.test(path))
            {
                res.writeHead(200, {'Content-Type': 'application/javascript'});
            }
            else
            {
                res.writeHead(200, {'Content-Type': 'application/x-shockwave-flash'});
            }
            fs.readFile('./' + path, function (err, data)
            {
                res.end(data);
            });
        }
        else
        {
            res.writeHead(200, {'Content-Type': 'text/html'});
            fs.readFile('./demo.html', function (err, data)
            {
                res.end(data.toString()
                   .replace(/\{hostname\}/g, req.headers.host.split(':')[0])
                   .replace(/\{namespace\}/g, path.replace(/^\/|\/.*/g, '')));
            });
        }
    });
}