//function convertEntityToMapLists(jsonstr, dataName) {
function convertEntityToMapLists(data, dataName) {
    //var data = JSON.parse(jsonstr);
    var a = { name: dataName, children: [] };
    var c = a.children;

    var o = { name: "aliases", children: [] };
    c.push(o);
    if (isValidList(data.aliases)) {
        for (i = 0, k = data.aliases.length; i < k; i++) {
            o.children.push({ name: concatObj(data.aliases[i]), children: [] })
        }
    }
    o = { name: "citizenships", children: [] }
    c.push(o);
    if (isValidList(data.citizenships)) {
        for (i = 0, k = data.citizenships.length; i < k; i++) {
            o.children.push({ name: concatObj(data.citizenships[i]), children: [] })
        }
    }
    o = { name: "identifications", children: [] }
    c.push(o);
    if (isValidList(data.identifications)) {
        for (i = 0, k = data.identifications.length; i < k; i++) {
            o.children.push({ name: concatObj(data.identifications[i]), children: [] })
        }
    }
    return a;
}

function concatObj(obj) {

    if (typeof obj === 'object') {
        var s = "";
        for (p in obj) {
            s += p + ": " + obj[p] + "; ";
        }
        return s;
    }

    return obj;
}

function isValidList(list) {
    return list && Array.isArray(list) && list.length > 0;
}
