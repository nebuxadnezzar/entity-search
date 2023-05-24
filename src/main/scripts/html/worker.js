var dataUrl = null;
var query = null;
var gridApi = null;
var intervalId = null;
var INTERVAL = 10000;

function getRowData(dataUrl) {
    if (!dataUrl) return;
    if (intervalId) { clearInterval(intervalId); }
    sendDataRequest(dataUrl, (data) => { processRowData(data, false); });

    function intervalFunc() {
        sendDataRequest(dataUrl, (data) => { processRowData(data, true); });
    }

    intervalId = setInterval(intervalFunc, INTERVAL);
}

function processRowData(data, skipColumnDefs) {
    var type = skipColumnDefs ? "updateData" : "setRowData";
    console.log("MSG TYPE: ", type)
    console.log(data);
    // JSON.parse(JSON.stringify(data)) - removes functions from the data
    //
    //postMessage({ "type": type, "value": JSON.parse(JSON.stringify(data)) });
    postMessage({ "type": type, "value": data });
}

function sendDataRequest(url, callback) {
    var httpRequest = new XMLHttpRequest();
    httpRequest.open('POST', url);
    httpRequest.setRequestHeader("Content-type", "application/json");
    httpRequest.send(JSON.stringify(query));
    console.log("!!! 3. SENDING DATA REQUEST\nQUERY: ", query);

    httpRequest.onreadystatechange = () => {
        if (httpRequest.readyState === 4) {
            if (httpRequest.status === 200) {
                callback(JSON.parse(httpRequest.responseText));
            } else {
                console.log(httpRequest.status, httpRequest.statusText)
            }
        }
    };
}

self.addEventListener("message", function (e) {
    switch (e.data.type) {
        case "setDataUrl":
            console.log("getting data url");
            //console.log(e.data);
            if (e.data.value) { dataUrl = e.data.value; }
            break;
        case "setGridApi":
            console.log("setting grid api");
            //console.log(e.data);
            if (e.data.value) { gridApi = e.data.value; }
            break;
        case "getRowData":
            console.log("getting initial data");
            // sendDataRequest( dataUrl, (data) => ( processRowData(data, false); ) );
            getRowData(dataUrl);
            break;
        case "query":
            console.log("received query ", e.data.value);
            query = e.data.value;
            break;
        default:
            console.log("unknown message type ", e.data);
            break;
    }
});