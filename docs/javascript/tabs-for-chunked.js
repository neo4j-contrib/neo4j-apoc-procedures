function tabTheSource($content) {
    var storedLanguage = getCodeExampleLanguage();
    var LANGUAGES = {
        'dotnet': 'C#',
        'java': 'Java',
        'javascript': 'JavaScript',
        'python': 'Python'
    };
    var $UL = $('<ul class="nav nav-tabs" role="tablist"/>');
    var $LI = $('<li role="presentation"/>');
    var $A = $('<a role="tab" data-toggle="tab" style="text-decoration:none;"/>');
    var $WRAPPER = $('<div class="tab-content content"/>');
    var snippets = [];
    var languageEventElements = {};

    var focusSelectedExample = function(e) {
        var target = $(e.target);
        var beforeTop = target.offset().top - $(window).scrollTop();
        setTimeout(function(){
            var newTop = target.offset().top - beforeTop;
            $('html,body').scrollTop(newTop);
        }, 1);
    }

    var selectTab = function (e) {
        var language = $(e.target).data('lang');
        var $elements = languageEventElements[language];
        for (var j = 0; j < $elements.length; j++) {
            $elements[j].tab('show');
        }
        if (storageAvailable('sessionStorage')) {
            sessionStorage.setItem('code_example_language', language);
        }
    }

    $('div.tabbed-example', $content).each(function () {
        var $exampleBlock = $(this);
        var title = $exampleBlock.children('div.example-title', this).first().text();
        var languages = [];
        var $languageBlocks = {};
        $(this).children('div.tabbed-example-contents').children('div.listingblock,div.informalexample[class*="include-with"]').each(function () {
            var $this = $(this);
            var language = undefined;
            if ($this.hasClass('listingblock')) {
                language = $('code', this).data('lang');
            } else {
                for (var key in LANGUAGES) {
                    if ($this.hasClass('include-with-' + key)) {
                        language = key;
                        break;
                    }
                }
            }
            languages.push(language);
            $languageBlocks[language] = $(this);
        });
        if (languages.length > 1) {
            snippets.push({
                '$exampleBlock': $exampleBlock,
                'languages': languages,
                '$languageBlocks': $languageBlocks
            });
        }
    });

    var idNum = 0;
    for (var ix = 0; ix < snippets.length; ix++) {
        var snippet = snippets[ix];
        var languages = snippet.languages;
        languages.sort();
        var $languageBlocks = snippet.$languageBlocks;
        var $exampleBlock = snippet.$exampleBlock;
        var idBase = 'tabbed-example-' + idNum++;
        var $wrapper = $WRAPPER.clone();
        var $ul = $UL.clone();

        for (var i = 0; i < languages.length; i++) {
            var language = languages[i];
            var $content = $($languageBlocks[language]);
            var id;
            if ($content.attr('id')) {
                id = $content.attr('id');
            } else {
                id = idBase + '-' + language;
                $content.attr('id', id);
            }
            $content.addClass('tab-pane').css('position', 'relative');
            var $li = $LI.clone();
            var $a = $A.clone();

            $a.attr('href', '#' + id).text(LANGUAGES[language]).data('lang', language).on('shown.bs.tab', selectTab).on('click', focusSelectedExample);

            if (language in languageEventElements) {
                languageEventElements[language].push($a);
            } else {
                languageEventElements[language] = [$a];
            }
            $wrapper.append($content);

            if (storedLanguage) {
                if (language === storedLanguage) {
                    $li.addClass('active');
                    $content.addClass('active');
                }
            } else if (i === 0) {
                $li.addClass('active');
                $content.addClass('active');
            }

            $li.append($a);
            $ul.append($li);
        }
        $exampleBlock.children('div.example-title', this).first().after($ul);
        $exampleBlock.append($wrapper);
    }
}

function storageAvailable(type) {
    try {
        var storage = window[type];
        var x = '__storage_test__';
        storage.setItem(x, x);
        storage.removeItem(x);
        return true;
    }
    catch(e) {
        return false;
    }
}

function getCodeExampleLanguage() {
    return storageAvailable('sessionStorage') ? sessionStorage.getItem('code_example_language') || false : false;
}
