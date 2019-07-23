if (!!document.getElementById("nav-version")) {
  document.getElementById("nav-version").onchange = function() {
    var selectedOption = this.value;
    window.location.href = "{{ site.url }}{{ site.baseurl }}/docs/" + selectedOption + "/{{ page.title | downcase | replace: ' ','-'}}" ;
  }
}
