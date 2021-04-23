// append rows and cols to table.data in page.html
function loadData() {
    data = document.getElementById("data");
    let index = 0;
    for (let row = 0; row < 2; row++) {
        let tr = document.createElement("tr");
        for (let col = 0; col < 2; col++) {
            td = document.createElement("td");
            td.appendChild(document.createTextNode(index + `foo
            bar -
            baz`));
            tr.appendChild(td);
            index++;
        }
        data.appendChild(tr);
    }
}


/*`<svg xmlns:xlink="http://www.w3.org/1999/xlink" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet" viewBox="0 0 36 36" height="16" width="16" fill="#007cbb" version="1.1">
                <title>thumbs-up-line</title>
                <path class="clr-i-outline clr-i-outline-path-1" d="M24,26c-2.92,1.82-7.3,4-9.37,4h-6a16.68,16.68,0,0,1-3.31-6.08A26.71,26.71,0,0,1,4,16h9V6a2.05,2.05,0,0,1,1.26-1.69c.77,2,2.62,6.57,4.23,8.72A11.39,11.39,0,0,0,24,16.91V14.78a9.13,9.13,0,0,1-3.91-3c-1.88-2.51-4.29-9.11-4.31-9.17A1,1,0,0,0,14.59,2C13.25,2.38,11,3.6,11,6v8H3a1,1,0,0,0-1,1,29,29,0,0,0,1.4,9.62c1.89,5.4,4.1,7.14,4.2,7.22a1,1,0,0,0,.61.21h6.42c2.43,0,6.55-2,9.37-3.63Z"></path><path class="clr-i-outline clr-i-outline-path-2" d="M34,31H27a1,1,0,0,1-1-1V14a1,1,0,0,1,1-1h7Zm-6-2h4V15H28Z"></path>
                <rect fill-opacity="0" height="36" width="36" y="0" x="0"></rect>
             </svg>
    <span style="position: relative; top: 2px;">osvaldo</span>`*/
