// Copyright 2016, EMC, Inc.

'use strict';

module.exports = amqpMessengerFactory;
amqpMessengerFactory.$provide = 'Task.Messengers.AMQP';
amqpMessengerFactory.$inject = [
    'Services.Messenger',
    'Protocol.Events',
    'Protocol.TaskGraphRunner',
    'Promise',
    'Assert',
    'Constants',
    'Errors'
];

function amqpMessengerFactory(
    messenger,
    eventsProtocol,
    taskGraphRunnerProtocol,
    Promise,
    assert,
    Constants,
    Errors
) {
    var exports = {};

    exports.subscribeRunTask = function(domain, callback) {
        var self = this;
        assert.string(domain);
        assert.func(callback);
        return messenger.subscribe(
            Constants.Protocol.Exchanges.Task.Name,
            domain + '.' + 'methods.run',
            callback
        );
    };

    exports.publishRunTask = function(domain, taskId, graphId) {
        assert.string(domain);
        return messenger.publish(
            Constants.Protocol.Exchanges.Task.Name,
            domain + '.' + 'methods.run',
            { taskId: taskId, graphId: graphId }
        );
    };

    exports.subscribeCancelTask = function(callback) {
        var self = this;
        assert.func(callback);
        return messenger.subscribe(
            Constants.Protocol.Exchanges.Task.Name,
            'methods.cancel',
            function(data) {
                if (!data.errName) {
                    return callback.call(self, data);
                }
                if (Errors[data.errName]) {
                    callback.call(self, new Errors[data.errName](data.errMessage));
                } else {
                    callback.call(self, new Error(data.errMessage));
                }
            }
        );
    };

    exports.publishCancelTask = function(taskId, errName, errMessage) {
        assert.uuid(taskId);
        return messenger.publish(
            Constants.Protocol.Exchanges.Task.Name,
            'methods.cancel',
            { taskId: taskId }
        );
    };

    exports.subscribeTaskFinished = function(domain, callback) {
        return eventsProtocol.subscribeTaskFinished(domain, callback);
    };

    exports.publishTaskFinished = function(
        domain,
        taskId,
        graphId,
        state,
        error,
        context,
        terminalOnStates
    ) {
        return eventsProtocol.publishTaskFinished(
                domain,
                taskId,
                graphId,
                state,
                error,
                context,
                terminalOnStates
        );
    };

    exports.subscribeRunTaskGraph = function(domain, callback) {
        return taskGraphRunnerProtocol.subscribeRunTaskGraph(domain, callback);
    };

    exports.subscribeCancelGraph = function(callback) {
        return taskGraphRunnerProtocol.subscribeCancelTaskGraph(callback);
    };

    exports.publishCancelGraph = function(graphId) {
        return taskGraphRunnerProtocol.cancelTaskGraph(graphId);
    };

    exports.start = function() {
        return Promise.resolve();
    };

    return exports;
}
