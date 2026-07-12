/* Elegansky screen loader — matches EleganskyBrain/src/components/common/screen-loader.tsx.
   Shows a pulsing Elegansky logo on page load and fades out after the window's
   `load` event, so the reference demo lands with the same splash the operator
   console will have. */
(function () {
  var el = document.createElement('div');
  el.id = 'elegansky-screen-loader';
  el.innerHTML =
    '<img alt="Elegansky" src="/static/demo/assets/media/app/elegansky-logo-256.png" />' +
    '<div>Loading...</div>';
  var s = document.createElement('style');
  s.textContent =
    '#elegansky-screen-loader{position:fixed;inset:0;z-index:9999;' +
      'display:flex;flex-direction:column;align-items:center;justify-content:center;' +
      'gap:12px;background:hsl(0 0% 100%);' +
      'transition:opacity .7s ease-in-out,visibility .7s ease-in-out;}' +
    '.dark #elegansky-screen-loader{background:hsl(240 10% 4%);}' +
    '#elegansky-screen-loader.is-done{opacity:0;visibility:hidden;pointer-events:none;}' +
    '#elegansky-screen-loader img{width:64px;height:64px;max-width:none;' +
      'animation:elegansky-pulse 1.4s ease-in-out infinite;}' +
    '#elegansky-screen-loader div{font-family:Inter,ui-sans-serif,system-ui,sans-serif;' +
      'font-size:.875rem;font-weight:500;color:hsl(240 3.8% 46.1%);}' +
    '.dark #elegansky-screen-loader div{color:hsl(240 5% 65%);}' +
    '@keyframes elegansky-pulse{' +
      '0%,100%{transform:scale(.92);opacity:.75;}' +
      '50%{transform:scale(1.08);opacity:1;}' +
    '}';
  var attach = function () {
    document.head.appendChild(s);
    document.body.insertBefore(el, document.body.firstChild);
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', attach);
  } else {
    attach();
  }
  window.addEventListener('load', function () {
    setTimeout(function () { el.classList.add('is-done'); }, 250);
    setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, 1200);
  });
})();
