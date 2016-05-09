// Copyright 2016, EMC, Inc.

'use strict';

describe('Task/TaskGraph AMQP messenger plugin', function () {
    var eventsProtocol;
    var taskGraphRunnerProtocol;
    var amqp;
    var messenger;
    var Constants;
    var taskId = 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA';
    var graphId ='BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB'
    var configuration = {
        get: sinon.stub().withArgs('taskgraph-messenger').returns('AMQP')
    };
    var eventsProtocolMock = {
        subscribeTaskFinished: sinon.stub().resolves(),
        publishTaskFinished: sinon.stub().resolves()
    };
    var taskGraphRunnerProtocolMock = {
        subscribeRunTaskGraph: sinon.stub().resolves(),
        subscribeCancelTaskGraph: sinon.stub().resolves(),
        cancelTaskGraph: sinon.stub().resolves()
    };

    before(function() {
        helper.setupInjector([
            helper.require('/lib/workflow/messengers/messenger-AMQP'),
            helper.di.simpleWrapper(configuration, 'Services.Configuration'),
            helper.di.simpleWrapper(eventsProtocolMock, 'Protocol.Events'),
            helper.di.simpleWrapper(taskGraphRunnerProtocolMock, 'Protocol.TaskGraphRunner'),
        ]);
        
        eventsProtocol = helper.injector.get('Protocol.Events');
        Constants = helper.injector.get('Constants');
        taskGraphRunnerProtocol = helper.injector.get('Protocol.TaskGraphRunner');
        amqp = helper.injector.get('Task.Messengers.AMQP');
        
        messenger = helper.injector.get('Services.Messenger');
        sinon.stub(messenger, 'publish');
        sinon.stub(messenger, 'subscribe');
    });

    afterEach(function() {
        _.forEach(eventsProtocol, function(method) {
            method.reset();
        });
        _.forEach(taskGraphRunnerProtocol, function(method) {
            method.reset();
        });
        messenger.publish.reset();
        messenger.subscribe.reset();
    });

    it('should subscribe to run exchange', function() {
        messenger.subscribe.resolves();
        var callback = function() {};
        return amqp.subscribeRunTask('default', callback)
        .then(function() {
            expect(messenger.subscribe).to.have.been.calledOnce;
            expect(messenger.subscribe).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'default.methods.run', 
                callback
            );
        });
    });

    it('should publish to run exchange', function() {
        messenger.publish.resolves();
        return amqp.publishRunTask('default', taskId, graphId)
        .then(function() {
            expect(messenger.publish).to.have.been.calledOnce;
            expect(messenger.publish).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'default.methods.run',
                { taskId: taskId, graphId: graphId }
            );
        });
    });

    it('should subscribe to cancel exchange', function() {
        messenger.subscribe = sinon.spy(function(exchange, name, callback) {
            callback({value:'value'});
            return Promise.resolve();
        });
        return amqp.subscribeCancelTask(function() {})
        .then(function() {
            expect(messenger.subscribe).to.have.been.calledOnce;
            expect(messenger.subscribe).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'methods.cancel'
            );
        });
    });
    
    it('should subscribe to cancel exchange with known error', function() {
        messenger.subscribe = sinon.spy(function(exchange, name, callback) {
            callback({errName:'MyError', errMessage:'errMessage'});
            return Promise.resolve();
        });
        return amqp.subscribeCancelTask(function() {})
        .then(function() {
            expect(messenger.subscribe).to.have.been.calledOnce;
            expect(messenger.subscribe).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'methods.cancel'
            );
        });
    });
    
    it('should subscribe to cancel exchange with unknown error', function() {
        messenger.subscribe = sinon.spy(function(exchange, name, callback) {
            callback({errName:'FakeError', errMessage:'errMessage'});
            return Promise.resolve();
        });
        return amqp.subscribeCancelTask(function() {})
        .then(function() {
            expect(messenger.subscribe).to.have.been.calledOnce;
            expect(messenger.subscribe).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'methods.cancel'
            );
        });
    });

    it('should publisc to cancel exchange', function() {
        return amqp.publishCancelTask(taskId)
        .then(function() {
            expect(messenger.publish).to.have.been.calledOnce;
            expect(messenger.publish).to.have.been.calledWith(
                Constants.Protocol.Exchanges.Task.Name,
                'methods.cancel',
                {taskId:taskId});
        });
    });

    it('should subscribe to task finished exchange', function() {
        var callback = function() {};
        return amqp.subscribeTaskFinished('default', callback)
        .then(function() {
            expect(eventsProtocol.subscribeTaskFinished).to.have.been.calledOnce;
            expect(eventsProtocol.subscribeTaskFinished).to.have.been.calledWith(
                'default',
                callback
            );
        });
    });

    it('should wrap the events protocol publishTaskFinished method', function() {
        return amqp.publishTaskFinished(
            'default', 'testtaskid', 'testgraphid', 'succeeded', null, ['failed', 'timeout'])
        .then(function() {
            expect(eventsProtocol.publishTaskFinished).to.have.been.calledOnce;
            expect(eventsProtocol.publishTaskFinished).to.have.been.calledWith(
                'default', 'testtaskid', 'testgraphid', 'succeeded', null, ['failed', 'timeout']
            );
        });
    });

    it('should wrap the taskGraphRunner protocol subscribeRunTaskGraph method', function() {
        var callback = function() {};
        return amqp.subscribeRunTaskGraph('default', callback)
        .then(function() {
            expect(taskGraphRunnerProtocol.subscribeRunTaskGraph).to.have.been.calledOnce;
            expect(taskGraphRunnerProtocol.subscribeRunTaskGraph)
                .to.have.been.calledWith('default', callback);
        });
    });

    it('should wrap the taskGraphRunner protocol subscribeCancelTaskGraph method', function(){
        var callback = function() {};
        return amqp.subscribeCancelGraph(callback)
        .then(function() {
            expect(taskGraphRunnerProtocol.subscribeCancelTaskGraph).to.have.been.calledOnce;
            expect(taskGraphRunnerProtocol.subscribeCancelTaskGraph)
                .to.have.been.calledWith(callback);
        });
    });

    it('should wrap the taskGraphRunner protocol cancelTaskGraph method', function(){
        return amqp.publishCancelGraph('testgraphid')
        .then(function() {
            expect(taskGraphRunnerProtocol.cancelTaskGraph).to.have.been.calledOnce;
            expect(taskGraphRunnerProtocol.cancelTaskGraph).to.have.been.calledWith('testgraphid');
        });
    });

    it('should return a promise from start method', function() {
        return expect(amqp.start()).to.be.fulfilled;
    });
});
