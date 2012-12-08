;(function($) {

var cssPrefix = null;

if ($.browser.mozilla) cssPrefix = "moz";
  else if ($.browser.webkit) cssPrefix = "webkit";
  else if ($.browser.opera) cssPrefix = "o";


function autolink(text) {
  return text.replace(/(https?:\/\/[-\w\.]+:?\/[\w\/_\-\.]*(\?\S+)?)/, "<a href='$1'>$1</a>");
}

function massageTweet(text) {
  text = text.replace(/^.* @\w+: /, "");

  return autolink(text);
}

function legitimate(text) {
  return !text.match(/le mapdb|mapdb le/i);
}

function buzz() {
  var $buzz = $("#buzz");

  if ($buzz.length == 0) return;

  var $ul = $buzz.find("ul");
  var count = 0;
  var limit = parseInt($buzz.attr("data-limit"));
  var page = $buzz.attr("data-page") || 1;
  var users = {};

  $.getJSON("http://search.twitter.com/search?q=mapdb+-RT&lang=en&rpp=30&format=json&page=" + page + "&callback=?", function(response) {
    $.each(response.results, function() {

      // Skip if the tweet is not Redis related.
      if (!legitimate(this.text)) { return; }

      // Don't show the same user multiple time
      if (users[this.from_user]) { return true; }

      // Stop when reaching the hardcoded limit.
      if (count++ == limit) { return false; }

      // Remember this user
      users[this.from_user] = true;

      $ul.append(
        "<li>" +
        "<a href='http://twitter.com/" + this.from_user + "/statuses/" + this.id_str + "' title='" + this.from_user + "'>" +
        "<img src='" + this.profile_image_url + "' alt='" + this.from_user + "' />" +
        "</a> " +
        massageTweet(this.text) +
        "</li>"
      );
    });
  });

  $buzz.find("> a.paging").click(function() {
    var $buzz = $(this).parent();
    $buzz.attr("data-page", parseInt($buzz.attr("data-page")) + 1);
    buzz();
    return false;
  });
}




$(document).ready(function() {
  buzz();
})

})(jQuery);
