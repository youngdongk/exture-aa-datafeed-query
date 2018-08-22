s.sendExLists = function(v, evar) {
    if (!s.eo && !s.lnk && v && evar) {
        var param = [];
        var d = new Date().getTime();
        var hit_id = d + "-" + d.toString(16) + Math.floor(1000 * Math.random()).toString(16);
        s[evar] = hit_id;
        param.push("i=" + encodeURIComponent(hit_id));
        param.push("v=" + encodeURIComponent(v));
        var img = new Image();
        var appurl = "https://MY_PROJECT_ID.appspot.com/b";
        var isrc = appurl + "?" + param.join("&");
        img.src = isrc;
    }
};
