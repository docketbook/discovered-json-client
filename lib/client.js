"use strict";
let async = require('async');
let consulClient = null;
let restifyLib = null;
let addressCache = {};
let refreshWindow = 5000;
let refreshStatus = {};
let maximumConcurrentUpdates = 1;

function performGetAddressForService(service, callback) {
	return consulClient.health.service({
		service: service,
		passing: true
	}, function(err, results) {
		if (err) return callback(err, null);
		let endpoints = [];
		let endpointObj = {};
		results.forEach(function(entry) {
			let obj = {
				address: entry.Service.Address,
				port: entry.Service.Port,
				tags: entry.Service.Tags,
				id: entry.Service.ID,
				fullAddress: entry.Service.Address + ':' + entry.Service.Port,
			}
			if (endpointObj[obj.fullAddress] === undefined) {
				endpointObj[obj.fullAddress] = true;
				endpoints.push(obj);
			}
		});
		addressCache[service] = endpoints;
		return callback(null, endpoints);
	});
}

function getAddresesForService(service, callback) {
	let statusObj = refreshStatus[service];
	if (statusObj === undefined) {
		refreshStatus[service] = {
			status: 'waiting',
			callbacks: [],
		}
		statusObj = refreshStatus[service];
	}
	statusObj.callbacks.push(callback);
	if (statusObj.status === 'waiting') {
		statusObj.status = 'pending';
		performGetAddressForService(service, function(err, endpoints) {
			statusObj.callbacks.forEach(function(callback) {
				return callback(err, endpoints);
			});
			statusObj.callbacks.splice(0, statusObj.callbacks.length);
			statusObj.status = 'waiting';
		});
	}
}

exports.getAddresesForService = getAddresesForService;

function buildClient(service, callback) {
	let addresses = addressCache[service];
	let address = addresses[Math.floor(Math.random() * addresses.length)];
	let url = 'http://' + address.address + ":" + address.port;
	return callback(null, restify.createJsonClient({
	  url:  url,
	}));
}

function refreshAddresses() {
	let services = Object.keys(addressCache);
	if (Object.keys(addressCache).length === 0) {
		return setRefreshTimer();
	}
	async.eachLimit(services, maximumConcurrentUpdates, 
	function(service, doneCallback) {
		getAddresesForService(service, doneCallback);
	}, function(err) {
		setRefreshTimer();
	});
}

function setRefreshTimer() {
	if (refreshWindow > 0) {
		setTimeout(refreshAddresses, refreshWindow);
	}
}

exports.getClient = function(service, callback) {
	if (addressCache[service] === undefined) {
		return getAddresesForService(service, function(err, results) {
			if (err) return callback(err, null);
			addressCache[service] = results;
			return buildClient(service, callback);
		});
	} else {
		return process.nextTick(() => {
	      return buildClient(service, callback);
	    });
	}
}

exports.getClients = function(services, callback) {
	let clients = [];
	async.map(services, function(service, doneCallback){
		exports.getClient(service, doneCallback);
	}, function(err, results) {
		return callback(err, results);
	});
}

exports.init = function(consul, restify, options) {
	consulClient = consul;
	restifyLib = restify;
	let opts = options;
	if (opts === undefined) {
		opts = {};
	}
	if ('refreshInterval' in opts) {
		refreshWindow = opts.refreshWindow * 1000;
	}
	if ('maximumConcurrentUpdates' in opts) {
		maximumConcurrentUpdates = opts.maximumConcurrentUpdates;
	}
	setRefreshTimer();
}