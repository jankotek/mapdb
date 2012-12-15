;(function($) {


function donate() {
    var theDiv = document.getElementById("search-form").parentNode;
    var email = 'jan'+'@'+'kotek.net';
    var content =  '<form id="donate-form" class="navbar-search pull-right" action="https://www.paypal.com/cgi-bin/webscr" method="post"><input type="hidden" name="cmd" value="_donations"><input type="hidden" name="business" value="'+email+'"><input type="hidden" name="lc" value="US"><input type="hidden" name="currency_code" value="EUR"><input type="hidden" name="bn" value="PP-DonationsBF:btn_donate_LG.gif:NonHosted"><input type="image" src="https://www.paypalobjects.com/en_US/i/btn/btn_donate_LG.gif" border="0" name="submit" alt="PayPal - The safer, easier way to pay online!"><img alt="" border="0" src="https://www.paypalobjects.com/en_US/i/scr/pixel.gif" width="1" height="1"></form>';
    theDiv.innerHTML+=  content;
}

$(document).ready(function() {
  donate();
})

})(jQuery);

