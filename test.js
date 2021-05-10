const notfis_generate = require("./src/index.js")

async function main() {
    const pathDiretory = 'C:/Users/beril/Desktop/TESTE';
    const listFiles = await notfis_generate.listFilesInDiretory(pathDiretory)
    listFiles.forEach(elem => console.log(elem))
    await notfis_generate.separateNfeXmlFilesByRegion(pathDiretory, 9);
    const jsons = await notfis_generate.xmlsInCSV(pathDiretory, ".xml", 8);
    await notfis_generate.json2CsvFile(jsons, pathDiretory + "/report")
    const notfis = await notfis_generate.generateNOTFIS(jsons, "08298621000105", "22270464000312")
    console.log(notfis)
    await notfis_generate.save(notfis[0], notfis[1] + '.txt')
}

main()