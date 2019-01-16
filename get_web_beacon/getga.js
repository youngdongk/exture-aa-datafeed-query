function() {
    return function(model) {
        var param = [];
        param.push('t=ga');
        param.push('l=' + encodeURIComponent(model.get('hitPayload')));
        var img = new Image();
        var appurl = 'https://YOUR-PROJECTID-HERE.appspot.com/b';
        var isrc = appurl + "?" + param.join("&");
        img.src = isrc;
  }
}
