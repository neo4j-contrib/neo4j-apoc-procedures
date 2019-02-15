jQuery( window ).load( function() {
  var location = window.location;
      versionSwitcher( jQuery );
} );

/**
 * Utility to browse different versions of the documentation. Requires the versions.js file loaded, which lists the
 * available (relevant) versions of a particular publication.
 */
function versionSwitcher( $ )
{
  $('.searchbox').hide();
  var MAX_STABLE_COUNT = 2;
  var DOCS_BASE_URL = window.docMeta.commonDocsBaseUri;
  var THIS_DOC_BASE_URI = window.docMeta.unversionedDocBaseUri;

  var currentVersion = window.docMeta.version;
  var currentPage = window.neo4jPageId;

  // TODO re-enable loadVersions();

  /**
   * Load an array of version into a div element and check if the current page actually exists in these versions.
   * Non-existing entries will be unlinked. Current version will be marked as such.
   */
  function loadVersions() {
    var $navHeader = $( 'header' );
    var $additionalVersions = $( '<ul class="dropdown-menu dropdown-menu-right" role="menu" aria-labelledby="dropdownMenu1"/>' );
    $.each( window.docMeta.availableDocVersions, function( index, version ) {
      if ( version === currentVersion ) {
        return;
      }
      else {
        addVersion( version, $additionalVersions );
      }
    } );

    var $dropdown = $( '<div id="additional-versions"><div class="dropdown"><a class="dropdown-toggle"id="dropdownMenu1" data-toggle="dropdown">Versions <i class="fa fa-caret-down"></i></a></div></div>' );
    $dropdown.children().first().append( $additionalVersions );
    $navHeader.append( $dropdown );
  }

  function addVersion( version, $container ) {
    var $optionWrapper = $( '<li />' );
    var $newOption = $( '<a role="menuitem">' + version + '</a>' ).appendTo( $optionWrapper );
    var url = THIS_DOC_BASE_URI + version + '/' + currentPage;
    $container.append( $optionWrapper );
    checkUrlExistence( url, function() {
        $newOption.attr( 'href', url );
        $newOption.attr( 'title', 'See this page in version ' + version + '.' );
      }, function() {
        $newOption.attr( 'title', 'This page does not exist in version ' + version + '.' );
        $optionWrapper.addClass( 'disabled' );
      }
    );
  }

  /**
   * Check if a specific URL exists. The success and failure functions will be automatically called on finish.
   */
  function checkUrlExistence( url, success, failure ) {
    var settings = {
      'type' : 'HEAD',
      'async' : true,
      'url' : url
    };
    if ( success )
      settings.success = success;
    if ( failure )
      settings.error = failure;
    $.ajax( settings );
  }
}
// vim: set ts=2 sw=2:
