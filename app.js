const csv = require('csv-parser');
const fs = require('fs');
let dataArray = [];
let idArray = [];
let dateArray = [];
let idAmount = 0;
let totalAmount = [];
const id = 'Customer Id';
const date = 'Order Placement Date';
const amount = 'Amount';

function sortID(a, b) {
    return a[id] - b[id];
}

fs.createReadStream('input.csv')
.pipe(csv())
.on('data', (row) => {
    dataArray.push(row);
})
.on('end', () => {
    dataArray.sort(sortID);
    for(let i = 0; i<dataArray.length; i++) {
        if(i == 0) {
            idArray.push([dataArray[i][id],[[dataArray[i][date],dataArray[i][amount]]]]);
            dateArray.push([dataArray[i][id],[]]);
            totalAmount[idAmount] = [];
            idAmount++;
            continue;
        }
        if(dataArray[i][id] !== dataArray[i-1][id]) {
            idAmount++;
            totalAmount[idAmount-1] = [];
            dateArray.push([dataArray[i][id],[]]);
            idArray.push([dataArray[i][id],[[dataArray[i][date],dataArray[i][amount]]]]);
        } else {
            idArray[idAmount-1][1].push([dataArray[i][date],dataArray[i][amount]]);
        }
    }
    idArray.forEach(function(element,number) {
        element[1].sort()
        let dateAmount = 0;
        for(let i = 0; i<element[1].length; i++) {
            if(i == 0) {
                dateArray[number][1].push([element[1][i][0].slice(0, 7),[element[1][i][1]]]);
                totalAmount[number][dateAmount] = 0;
                totalAmount[number][dateAmount] += parseInt(element[1][i][1]);
                dateAmount++;
                continue;
            }
            if(element[1][i][0].slice(0, 7) != element[1][i-1][0].slice(0, 7)) {
                dateAmount++;
                dateArray[number][1].push([element[1][i][0].slice(0, 7),[element[1][i][1]]]);
                totalAmount[number][dateAmount-1] = 0;
                totalAmount[number][dateAmount-1] += parseInt(element[1][i][1]);
            } else {
                dateArray[number][1][dateAmount-1][1].push(element[1][i][1]);
                totalAmount[number][dateAmount-1] += parseInt(element[1][i][1]);
            }
        }  
        for(let i = 0; i<dateArray[number][1].length; i++) {
            let points = 0;
            for(let j = 0; j<dateArray[number][1][i][1].length; j++) {
                if(dateArray[number][1][i][1][j] > 1000) {
                    points+=20;
                } else if ((j+1)%3 == 0 && j>0) {
                    points+=10;
                }
                else if  (i > 0 && dateArray[number][1][i][1][j] > 100  && totalAmount[number][i-1]>2000) {
                    points+=5;
                }
            }  
            let newMonth = parseInt(dateArray[number][1][i][0].slice(5,7))+1;
            let newYear = parseInt(dateArray[number][1][i][0].slice(0,4));
            if(newMonth > 12) {
                newYear++;
                newMonth = '01';
            } else if (newMonth < 10) {
                newMonth = '0'+newMonth;
            }
            let newDate = `${newYear}-${newMonth}-01`;
            console.log(`${dateArray[number][0]},${newDate},${points}`);          
        }
      });
});