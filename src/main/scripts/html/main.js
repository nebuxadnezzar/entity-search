(function () {
    var grid = null;
    var callApi = null;
    var gridOptions = {
        defaultColDef: {
            width: 150,
            sortable: true,
            resizable: true,
            filter: true,
            cellRenderer: "agAnimateShowChangeCellRenderer"
        },
        animateRows: true,
        enableBrowserTooltips: true,
        rowHeight: 33,
        rowSelection: "multiple",
        pagination: true,
        paginationPageSize: 30,
        debug: true,
        onGridReady: function (params) {
            colApi = params.columnApi;
            console.log("PARAMS\n");
            console.log(params);
        },
        getRowId: function (params) {
            //console.log("DATA WITH ID: ", params.data._id)
            return params.data._id;
        },
        onCellMouseDown: function (e) { console.log("MOUSE DOWN: ", createIdQuery(e.data._id)) }
    };

    var worker;
    var dataUrl;
    var columnDefs;

    function startWorker() {
        worker = new Worker("./worker.js");
        console.log("WORKER: ", worker)
        console.log("column Defs " + columnDefs)
        worker.onmessage = function (e) {
            console.log("MESSAGE RECEIVED: " + e.data.type)
            switch (e.data.type) {
                case "getDataUrl":
                    console.log("getDataUrl received, got data url request!!!\n");
                    dataUrl = document.getElementById("data-url").value;
                    worker.postMessage({ "type": "setDataUrl", "value": dataUrl });
                    break;
                case "setRowData":
                    console.log("setRowData received, setting row data");
                    //console.log( e.data.value );
                    //gridOptions.api.setColumnDefs(addRenderer(columnDefs));
                    gridOptions.api.setColumnDefs(columnDefs);
                    gridOptions.api.setRowData(e.data.value.data);
                    break;
                case "updateData":
                    console.log("updateData received, setting row data");
                    var ret = splitData(e.data.value.data);
                    // if (ret.adds.length) { gridOptions.api.updateRowData({ add: ret.adds }); }
                    //console.log("ADDS: ", ret.adds)
                    if (ret.adds.length) { gridOptions.api.applyTransaction({ add: ret.adds }); }
                    if (ret.updates.length) { gridOptions.api.applyTransactionAsync({ update: ret.adds }); }
                    break;
                default:
                    console.log("unrecognised event type " + e.type);
            }
        }
    }

    function splitData(data) {
        var adds = [], updates = [];
        for (var i = 0, k = data.length; i < k; i++) {
            var d = data[i];
            //console.log(d._id)
            if (gridOptions.api.getRowNode(d._id)) { updates.push(d); }
            //if (gridOptions.api.getRowNode(d.data_source_id)) { updates.push(d); }
            else { adds.push(d); }
            //rowNode.setDataValue('price', newPrice);
        }
        return { "adds": adds, "updates": updates };
    }

    function clearData() {
        const rowData = [];
        gridOptions.api.forEachNode(function (node) {
            rowData.push(node.data);
        });
        const res = gridOptions.api.applyTransaction({
            remove: rowData,
        });
    }

    function createIdQuery(id) {
        var query = {
            "_filterFields_": [],
            "_limit_": 2,
            "_type_": "and"
        };
        query._id = [id];
        return query;
    }
    // if data contains html links they will not work unless renderer is added
    // the simplest renderer just returns data value which could be a link
    // cell renderer must return DOM element
    //
    function addRenderer(columnDefs) {
        var cd = columnDefs;
        for (var i = 0, k = cd.length; i < k; i++) {
            var item = cd[i], f = item.field;
            item.cellRenderer = function (params) { return params.value; }
            item.cellRendererParams = { "field": f };
        }
        return cd;
    }

    function initLiveStreamUpdates() {
        var eGridDiv = document.querySelector("#live-stream-updates-grid");

        if (!worker) { startWorker(); }
        if (!grid) { grid = new agGrid.Grid(eGridDiv, gridOptions); }
    }

    if (document.readyState == "complete") {
        initLiveStreamUpdates();
    } else {
        document.addEventListener("readystatechange", initLiveStreamUpdates);
    }

    window.pushDataUrl = function (dataUrl) {
        console.log("POSTING DATA URL: " + dataUrl)
        worker.postMessage({ "type": "setDataUrl", "value": dataUrl });
    }
    window.getRowData = function () {
        worker.postMessage({ "type": "getRowData" });
    }

    window.pushColumnDefs = function (cDefs) {
        columnDefs = cDefs;
        console.log("PUSHING COLUMN DEFS: ", columnDefs)
    }

    window.pushQuery = function (query) {
        worker.postMessage({ "type": "query", "value": query });
    }

    window.pushDataSetName = function (dataSetName) {
        worker.postMessage({ "type": "setDataSetName", "value": dataSetName });
    }

    window.setClickHandler = function (clickHandler) {
        gridOptions.onCellMouseDown = function (e) {
            console.log("MOUSE DOWN: ", e.data, "clickHandler ", clickHandler);
            if (clickHandler) {
                clickHandler(e.data);
            }
        }
    }
    window.clearData = function () { clearData(); }

})();