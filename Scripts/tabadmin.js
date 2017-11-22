const fs = require('fs');
const hd = process.argv[2];
const version = process.argv[3];

const text1 = '@"'+hd.toUpperCase()+':\\Tableau Server\\'+version+'\\bin\\tabadmin.exe" %*'
const text0 = '@"C:\\Program Files\\Tableau Server\\'+version+'\\bin\\tabadmin.exe" %*'

const text = (hd.toLowerCase === 'c' ? text0 : text1);

fs.writeFile('tabadmin.bat', text, function(err){
    console.log(err);
});
