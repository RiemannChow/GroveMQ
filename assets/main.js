// Mobile navigation toggle
(function(){
  var toggle=document.querySelector('.menu-toggle');
  var links=document.querySelector('.nav-links');
  if(!toggle||!links)return;
  toggle.addEventListener('click',function(){
    links.classList.toggle('open');
  });
  links.querySelectorAll('a').forEach(function(a){
    a.addEventListener('click',function(){links.classList.remove('open')});
  });
})();

// Mark active nav item by pathname
(function(){
  var path=location.pathname.split('/').pop()||'index.html';
  document.querySelectorAll('.nav-links a[data-page]').forEach(function(a){
    if(a.getAttribute('data-page')===path)a.classList.add('active');
  });
})();

// Scroll reveal: add class only when JS is active, so content stays visible
// if scripting is disabled.
(function(){
  document.documentElement.classList.add('js');
  if(!('IntersectionObserver' in window))return;
  var els=document.querySelectorAll('.reveal');
  var io=new IntersectionObserver(function(entries){
    entries.forEach(function(e){
      if(e.isIntersecting){e.target.classList.add('show');io.unobserve(e.target)}
    });
  },{threshold:.12});
  els.forEach(function(el){io.observe(el)});
})();

// Copy code on click
(function(){
  document.querySelectorAll('.code .copy').forEach(function(btn){
    btn.addEventListener('click',function(){
      var pre=btn.closest('.code').querySelector('pre');
      if(!pre)return;
      var text=pre.innerText;
      if(navigator.clipboard&&navigator.clipboard.writeText){
        navigator.clipboard.writeText(text).then(function(){
          var old=btn.textContent;btn.textContent='Copied';
          setTimeout(function(){btn.textContent=old},1500);
        });
      }
    });
  });
})();
