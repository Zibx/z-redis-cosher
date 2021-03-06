/**
 * Created by Ivan on 11/19/2014.
 */
var redis = require("redis");

var Z = require('z-lib' ),
    UUID = require('z-lib-uuid');
var Cosher = module.exports = function( cfg ){
    this.unique = UUID.getRandom();

    Z.apply( this, cfg );
    this._initConnections();
    this.hash = this.hash || {};
    this.timeouts = {};
    this._subscribe();
    this.timeout && this._timeout();
};
Cosher.prototype = {
    _initConnections: function(  ){
        Cosher.prototype.client = Cosher.prototype.client || redis.createClient(this.connectCfg.port,this.connectCfg.host);
        Cosher.prototype.sclient = Cosher.prototype.sclient || redis.createClient(this.connectCfg.port,this.connectCfg.host);
    },
    _timeout: function(  ){
        setTimeout( function(  ){
            this._findRetarded();
            this._timeout();
        }.bind(this), this.timeout*1000);
    },
    _findRetarded: function(  ){
        var i, x = 0, c = 0, g = 0,
            timeouts = this.timeouts,
            hash = this.hash,
            item,
            epsonSince = +new Date() - this.timeout*1000,
            d = (Math.log(Math.random()+1)/Math.log(2)*5+1)|0;

        for( i in timeouts ) if( timeouts.hasOwnProperty(i) ){
            x++;
            if( x%d===0 ){
                c++;
                if( c > 100 )
                    if( g < 25 ){
                        break;
                    }else{
                        c = 0;
                    }

                item = timeouts[i];
                if( item < epsonSince ){
                    delete timeouts[i];
                    delete hash[i];
                }
            }
        }
    },
    _subscribe: function(  ){
        var name = this.name,
            hash = this.hash,
            unique = this.unique,
            timeouts = this.timeouts,
            _self = this;
        this.sclient.psubscribe( this.name +'/*' );
        this.sclient.on('pmessage', function( pattern, channel, message ){
            var tokens = channel.split( '/' );
            if( tokens[0] === name && (tokens[1] in hash)) {
                if (tokens[2] !== unique) {
                    delete hash[tokens[1]];
                    delete timeouts[tokens[1]];
                    _self.onchange && _self.onchange(true);
                }
            }
        });
    },
    retry: 10,
    change: function( id ){
        var el = this.hash[id];
        if( el ){
            this.client.hset( [this.name, id, JSON.stringify( el )], function(){} );
            this.client.publish( this.name + '/' + id + '/' + this.unique, '' );
        }
        this.onchange && this.onchange();
    },
    remove: function( id ){
        this.client.hdel( [this.name, id], function(){} );
        this.client.publish( this.name + '/' + id + '/' + this.unique, '' );
        delete this.hash[id];
        delete this.timeouts[id];
        this.onchange && this.onchange();
    },
    _preQuery: function( id, cb ){
        this.preQList = this.preQList || [];
        cb && this.preQList.push([id, cb]);

        if( !this.preQuered ){
            this.preQuered = 1;
            var self = this;
            this.preQuery( function(){
                self.preQuered = true;
                self._preQuery();
            } );
        }else if( this.preQuered === true ){
            var el;
            if( this.preQList )
                while(el = this.preQList.pop())
                    el[1] && this.get(el[0],el[1]);
        }
    },
    get: function( id, cb ){
        if( this.preQuery && this.preQuered !== true )
            return this._preQuery(id, cb);
        var hash = this.hash, item,
            timeouts = this.timeouts,
            _self = this,
            later = function(  ){
                cb( !(id in hash), hash[id] );
                timeouts[id] = +new Date();
            },
            client = this.client;
        if( item = hash[id] ){
            if( item instanceof Z.wait.constructor ){
                item.after( later );
            }else
                later();
        }else{
            hash[id] = Z.wait(later);
            hash[id].add();
            var after = function( val ){
                    var waiter = hash[id];
                    hash[id] = val;
                    waiter && waiter.done && waiter.done();
                },
                myName = '!wait:'+this.unique,
                retry = this.retry;

            client.hget( [_self.name, id], function( err, val ){
                if( !err && val !== null ){
                    if( val.indexOf('!wait:') === 0 ){
                        setTimeout(_self.get.bind(_self,id,cb),retry);
                    }else{
                        after(JSON.parse(val));
                    }
                }else{
                    client.hsetnx([_self.name, id, myName], function( err, reply ){
                        if( reply === 1 ){
                            _self.query(id, function(qVal){
                                after(qVal);
                                client.hset( [_self.name, id, JSON.stringify( qVal )], function(){} );
                            });
                        }else{
                            setTimeout(_self.get.bind(_self,id,cb),retry);
                        }
                    });
                }
            } );
        }
    }
};
Cosher.sync = function (cfg) {
    Z.apply(this, cfg);
    var _self = this;
    this.redis = new Cosher({
        name: this.name,
        timeout: 60*24,
        connectCfg: this.connectCfg,
        query: function (id, cb) {
            _self.query(function (data) {
                cb(data);
            });
        },
        onchange: function (outer) {
            if(outer) {
                _self.init();
            }
        }
    });
};
Cosher.sync.prototype = {
    changed: function () {
/*        console.log('');
        console.log('changed', this.uid)*/
        this.redis.remove('all');
        this.init();
    },
    init: function () {
        var _self = this;
        this.redis.get('all', function (err, data) {
            if(!err && data) {
                if(data._actions){
                    delete _self.redis.hash.all;
                    setTimeout(_self.init.bind(_self),40);
                }else {
                    _self.change(data);
                }
            }
        })
    }
};