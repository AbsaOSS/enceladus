---
layout: docs
title: Components - Menas
version: '2.0.0'
categories:
    - '2.0.0'
    - components
---
## API

### Monitoring endpoints

All `/admin` endpoints except `/admin/health` require authentication (and will require strict permissions once [Authorization]({{ site.github.issues_url }}/30) is implemented)
* `GET /admin` - list of all monitoring endpoints
* `GET /admin/heapdump` - downloads a heapdump of the application
* `GET /admin/threaddump` - list of the threaddump of the application
* `GET /admin/loggers` - list of all the application loggers and their log levels
* `POST /admin/loggers/{logger}` - change the log level of a logger in runtime
* `GET /admin/health` - get a detailed status report of the application's health:
```json
{
  "status": "UP",
  "details": {
    "HDFSConnection": {
      "status": "UP"
    },
    "MongoDBConnection": {
      "status": "UP"
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 1000240963584,
        "free": 766613557248,
        "threshold": 10485760
      }
    }
  }
}
```
