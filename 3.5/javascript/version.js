window.docMeta = (function () {
  var version = '3.4';
  var name = 'APOC';
  var href = window.location.href;
  var len = href.indexOf('/' + version) != -1 ? href.indexOf('/' + version) : href.length -1;
  return {
    name: name,
    version: version,
    availableDocVersions: ["3.3", "3.4", "3.5"],
    thisPubBaseUri: href.substring(0,len) + '/' + version,
    unversionedDocBaseUri: href.substring(0, len) + '/',
    commonDocsBaseUri: href.substring(0, href.indexOf(name) - 1)
  }
})();

(function () {
  var baseUri = window.docMeta.unversionedDocBaseUri; // + window.location.pathname.split(window.docMeta.name + '/')[1].split('/')[0] + '/';
  var docPath = window.location.href.replace(baseUri, '');
  window.neo4jPageId = docPath;
})();
// vim: set sw=2 ts=2:
