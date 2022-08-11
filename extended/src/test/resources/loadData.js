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
