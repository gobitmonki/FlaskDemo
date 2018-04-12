function getSettings(){
    $.ajax({
        type: "post",
        url: url,
        success: function(result){
            $("#property").text(result);
        }
    })
}
$(document).ready(function(){
    url="http://106.14.152.35:5000/getSettings"
    console.log("index ready");
    getSettings(url);
})


