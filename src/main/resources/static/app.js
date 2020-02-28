// **NOTE reference
// https://github.com/spring-guides/gs-messaging-stomp-websocket/blob/master/complete/src/main/resources/static/app.js

var stompClient = null;

function setConnected(connected) {
    $("#connect").prop("disabled", connected);
    $("#disconnect").prop("disabled", !connected);
//    if (connected) { $("#conversation").show(); }
//    else { $("#conversation").hide(); }
//    $("#greetings").html("");
    $( "#sendJobId" ).prop("disabled", !connected);
    $( "#sendJobAll" ).prop("disabled", !connected);
    $( '#jobOutput tbody tr').remove();
}

function connect() {
    var socket = new SockJS('/agens-spark');
    stompClient = Stomp.over(socket);
    stompClient.connect({}, function (frame) {
        setConnected(true);
        console.log('Connected: ' + frame);

        // **receiver : destination of "agenspop" channel
        stompClient.subscribe('/topic/greetings', function (greeting) {
            showGreeting(JSON.parse(greeting.body).content);
        });
        // **receiver : job status array
        stompClient.subscribe('/topic/jobstatus', function (jobMessages) {
            if( !jobMessages.body.trim() ) return;          // null or empty
            var msgArray = JSON.parse(jobMessages.body)
            if( Array.isArray(msgArray) ){
                for(var i=0; i<msgArray.length; i++){
                    showJobStatus(msgArray[i]);
                }
            }
            else showJobStatus(msgArray);
        });
    });
}

function disconnect() {
    if (stompClient !== null) {
        stompClient.disconnect();
    }
    setConnected(false);
    console.log("Disconnected");
}

// **sender : call JobController API by @MessageMapping("/hello") ("/app" is prefix)
function sendName() {
    stompClient.send("/agens/hello", {}, JSON.stringify({'name': $("#name").val()}));
}

function showGreeting(message) {
    $("#greetings").append("<tr><td>" + message + "</td></tr>");
}

$(function () {
    $("form").on('submit', function (e) {
        e.preventDefault();
    });
    $( "#connect" ).click(function() { connect(); });
    $( "#disconnect" ).click(function() { disconnect(); });
//    $( "#send" ).click(function() { sendName(); });

    $( "#sendJobId" ).click(function() { sendJobId(); });
    $( "#sendJobAll" ).click(function() { sendJobAll(); });
    $( "#listClear" ).click(function() { jobListClear(); });

    $( "#cmdDropkeys" ).click(function() { cmdDropkeys(); });
    $( "#cmdCount" ).click(function() { cmdCount(); });
    $( "#cmdIndegree" ).click(function() { cmdIndegree(); });
    $( "#cmdOutdegree" ).click(function() { cmdOutdegree(); });
    $( "#cmdPagerank" ).click(function() { cmdPagerank(); });
    $( "#cmdComponent" ).click(function() { cmdComponent(); });
    $( "#cmdScomponent" ).click(function() { cmdScomponent(); });

    $('input#valDatasource').blur(function() { checkDatasource(); });
});

///////////////////////////////////////////

function showJobStatus(msg) {
    var content = (msg.hasOwnProperty('jobId') ? msg.jobId : "")
        +"</td><td>"+(msg.hasOwnProperty('currentStep')&&msg.hasOwnProperty('totalSteps')
            ? msg.currentStep+"/"+msg.totalSteps : "_/_")
        +"</td><td>"+(msg.hasOwnProperty('elapsedTime') ? msg.elapsedTime : "")
        +"</td><td>"+(msg.hasOwnProperty('state') ? msg.state : "")
        +"</td><td>"+(msg.hasOwnProperty('progress') ? msg.progress : "");
    $("#jobOutput").append("<tr><td>" + content + "</td></tr>");
}

function sendJobId() {
    stompClient.send("/agens/status", {}, $("#jobId").val());
}

function sendJobAll() {
    stompClient.send("/agens/status", {}, "all");
}

function jobListClear(){
    $( '#jobOutput tbody tr').remove();
}

///////////////////////////////////////////

function checkDatasource(){
    var ds = $('input#valDatasource').val();
    if( ds.toLowerCase() == "northwind" ){
        $('input#valParam1').val("supplier,category,employee");
        $('input#valParam2').val("reports_to,part_of,supplies,sold");
    }
    else if( ds.toLowerCase() == "airroutes" ){
        $('input#valParam1').val("continent,country");
        $('input#valParam2').val("contains");
    }
    else{
        $('input#valParam1').val("");
        $('input#valParam2').val("");
    }
}

function cmdDropkeys(){
    var ds = $('input#valDatasource').val();
    var index = $('input#valParam1').val();
    var keys = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/"+index+"/dropkeys";
    console.log('cmdDropkeys:', ds, url, keys);
    $.ajax({
        url: url,
        data: { q: keys },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Dropkeys 요청이 완료되었습니다!");
    });
}

function cmdCount(){
    var ds = $('input#valDatasource').val();
    var url = "/api/spark/"+ds+"/count";
    console.log('cmdCount:', ds, url);
    $.ajax({
        url: url,
        // data: { q: "홍길동" },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Count 요청이 완료되었습니다!");
    });
}

function cmdIndegree(){
    var ds = $('input#valDatasource').val();
    var excludeV = $('input#valParam1').val();
    var excludeE = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/indegree";
    console.log('cmdIndegree:', ds, excludeV, excludeE);
    $.ajax({
        url: url,
        data: { excludeV: excludeV, excludeE: excludeE },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Indegree 요청이 완료되었습니다!");
    });
}

function cmdOutdegree(){
    var ds = $('input#valDatasource').val();
    var excludeV = $('input#valParam1').val();
    var excludeE = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/outdegree";
    console.log('cmdOutdegree:', ds, excludeV, excludeE);
    $.ajax({
        url: url,
        data: { excludeV: excludeV, excludeE: excludeE },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Outdegree 요청이 완료되었습니다!");
    });
}

function cmdPagerank(){
    var ds = $('input#valDatasource').val();
    var excludeV = $('input#valParam1').val();
    var excludeE = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/pagerank";
    console.log('cmdPagerank:', ds, excludeV, excludeE);
    $.ajax({
        url: url,
        data: { excludeV: excludeV, excludeE: excludeE },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Pagerank 요청이 완료되었습니다!");
    });
}

function cmdComponent(){
    var ds = $('input#valDatasource').val();
    var excludeV = $('input#valParam1').val();
    var excludeE = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/component";
    console.log('cmdComponent:', ds, excludeV, excludeE);
    $.ajax({
        url: url,
        data: { excludeV: excludeV, excludeE: excludeE },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Component 요청이 완료되었습니다!");
    });
}

function cmdScomponent(){
    var ds = $('input#valDatasource').val();
    var excludeV = $('input#valParam1').val();
    var excludeE = $('input#valParam2').val();
    var url = "/api/spark/"+ds+"/scomponent";
    console.log('cmdScomponent:', ds, excludeV, excludeE);
    $.ajax({
        url: url,
        data: { excludeV: excludeV, excludeE: excludeE },
        method: "GET",
        dataType: "text"
    })
    .done(function(json) {
        console.log(json);
        $("textarea#comment").text(json);
    })
    .fail(function(xhr, status, errorThrown) {
        $("textarea#comment").text(status + "\n==> "+errorThrown);
    })
    .always(function(xhr, status) {
        var content = $("textarea#comment").text();
        $("textarea#comment").text(content+"\n==> Scomponent 요청이 완료되었습니다!");
    });
}
