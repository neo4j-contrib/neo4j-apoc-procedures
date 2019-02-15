/**
 * JavaScript for navigation in multi-page editions of Neo4j documentation.
 */

function isElementInViewport (el) {
    if (typeof jQuery === "function" && el instanceof jQuery) {
        el = el[0];
    }
    var rect = el.getBoundingClientRect();
    return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) && /*or $(window).height() */
        rect.right <= (window.innerWidth || document.documentElement.clientWidth) /*or $(window).width() */
    );
}

$(document).ready(function() {
    var $title = $(
            'h1,h2,h3,h4'
            ).first();
    var $navtitle = $('.nav-title');
    var visible = isElementInViewport($title);
    if (visible) {
        $navtitle.hide();
    }
    $navtitle.removeClass('hidden');

    function showHide(nowVisible) {
        if ($(window).width() >= 768 && visible !== nowVisible) {
            $navtitle.fadeToggle();
            visible = !visible;
        }
    }
    var timeoutId = null;
    addEventListener("scroll", function() {
        if (timeoutId) clearTimeout(timeoutId);
            timeoutId = setTimeout(showHide, 200, isElementInViewport($title));
    }, true);

    setNavIconColor();
});

function setNavIconColor() {
    var color = null;
    $('.nav-previous > a, .nav-next > a').hover(function (){
        $me = $(this);
        $me.children('span.fa').css('border-color', $me.css('color'));
    }, function(){
        $(this).children('span.fa').css('border-color', "");
    });
}

// Highlight the current chapter/section in the TOC
function highlightToc() {
    var toc = document.querySelector('nav.toc > ul.toc');
    var allAnchors = toc.getElementsByTagName('a');
    var thisAnchor;
    var urlDissimilarity = 1000;
    for (i=0; i < allAnchors.length; i++) {
        var candidate = allAnchors.item(i).href;
        var test = document.URL.replace(candidate);
        // console.log('candidate:', candidate, 'test:', test, 'urlDissimilarity:', test.length);
        if (test.length < urlDissimilarity && test !== document.URL) {
            urlDissimilarity = test.length;
            thisAnchor = allAnchors.item(i);
        }
    };

    // console.log("[XXX] RESULT:", thisAnchor, "dissimilarity:", urlDissimilarity);

    if (thisAnchor !== undefined) {
        thisAnchor.parentElement.classList.add('active-nested-section');
        var topLevel = thisAnchor;
        while (topLevel.parentElement !== toc) {
            // console.log("traversing up:", topLevel);
            topLevel = topLevel.parentElement;
        }
        if (thisAnchor !== topLevel) {
            // console.log("highlighting:", topLevel);
            topLevel.classList.add('active-toplevel-section');
        }
    }
}

// Highlight the active publication in the docs library header
function highlightLibraryHeader() {
    var thisName = window.docMeta.name
    var thisEntry;
    $('header > ul.documentation-library').children('li').children('a').each(
        function (key, value) {
            var href = $(this).attr('href');
            if (href.includes(thisName)) {
                $(this).css({
                    color: '#428bca',
                    backgroundColor: 'rgb(66, 139, 202, 0.05)',
                    borderBottom: '2px solid #428bca',
                    padding: '4px',
                    marginBottom: '-6px'
                });
            }
            // console.log('href:', href, 'thisUrl:', thisUrl, 'thisName:', thisName);
        }
    );
}
