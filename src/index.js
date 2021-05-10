const xml2js = require('xml2js');
const fsp = require('fs').promises;
const parser = new xml2js.Parser({ attrkey: "ATTR" });
const moveFile = require('move-file');
const converter = require('json-2-csv');

function create() {

    async function listFilesInDiretory(diretoryPath, files = []) {
        let filesList = await fsp.readdir(diretoryPath);
        for (let k in filesList) {
            let stat = await fsp.stat(diretoryPath + '/' + filesList[k]);
            if (stat.isDirectory())
                await listFilesInDiretory(diretoryPath + '/' + filesList[k], files);
            else
                files.push(diretoryPath + '/' + filesList[k]);
        }
        return files;
    }

    async function separateNfeXmlFilesByRegion(mainPath, level, fileType = ".xml") {
        let paths = await listFilesInDiretory(mainPath);
        let files = paths.filter(elem => elem.split('/').length < level).filter(elem => elem.includes(fileType))
        var qtdFilesOk = 0;
        var qtdFilesError = 0;
        var max = files.length;
        if (max >= 60000) max = 60000;
        for (let i = 0; i < max; i++) {
            const fileName = files[i]
            const file = await fsp.readFile(fileName, "utf8")
            let xmlString = await parser.parseStringPromise(file)
            var region = xmlString.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].UF[0];
            let fileDestinationName = fileName.split('/');
            fileDestinationName = fileDestinationName[fileDestinationName.length - 1];
            await moveFile(fileName, mainPath + '/' + region + '/' + fileDestinationName);
        }
        console.log("qtd files error: ", qtdFilesError, "qtd files ok:", qtdFilesOk)
    }

    async function xmlToJson(arq) {
        let xml_string = await fsp.readFile(arq, "utf8");
        var ret = null;
        parser.parseString(xml_string, function (error, result) {
            if (error === null) {
                ret = result
            }
        });
        return ret;
    }

    async function save(content, log_path) {
        let strContent = String(content)
        await fsp.writeFile(log_path, strContent, (err) => {
            if (err) console.log(err);
        });
    }

    async function xmlsProductsInCSV(mainPath, fileType = ".xml", level = 1) {
        let paths = await listFilesInDiretory(mainPath);
        let files = paths.filter(elem => elem.split('/').length < level).filter(elem => elem.includes(fileType))
        var jsons = [];
        for (let i = 0; i < files.length; i++) {
            try {
                const json = await xmlToJson(files[i]);
                const products = json.nfeProc.NFe[0].infNFe[0].det.map(item => {
                    const keyICMS = Object.keys(item.imposto[0].ICMS[0])[0].toString()
                    return Object.assign({}, item.prod[0],
                        item.imposto[0].ICMSUFDest ? item.imposto[0].ICMSUFDest[0] : null,
                        item.imposto[0].ICMS[0] ? item.imposto[0].ICMS[0][keyICMS][0] : null)
                });
                for (let j = 0; j < products.length; j++) {
                    const product = products[j];

                    const transportadora = json.nfeProc.NFe[0].infNFe[0].transp[0].transporta;
                    let CNPJTransp = '';
                    let xNomeTransp = '';
                    let IETransp = '';
                    let UFTransp = '';
                    let xEnderTransp = '';
                    let xMunTransp = '';
                    if (transportadora) {
                        CNPJTransp = transportadora[0].CNPJ ? transportadora[0].CNPJ[0] : null;
                        xNomeTransp = transportadora[0].xNome ? transportadora[0].xNome[0] : null;
                        IETransp = transportadora[0].IE ? transportadora[0].IE[0] : null;
                        UFTransp = transportadora[0].UF ? transportadora[0].UF[0] : null;
                        xEnderTransp = transportadora[0].xEnder ? transportadora[0].xEnder[0] : null;
                        xMunTransp = transportadora[0].xMun ? transportadora[0].xMun[0] : null;
                    }
                    const json_aux = {
                        "CNPJEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].CNPJ[0],
                        "xLgr": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].xLgr[0],
                        "xNomeEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].xNome[0],
                        "nroEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].nro[0],
                        "xMunEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].xMun[0],
                        "UFEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].UF[0],
                        "CEPEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].CEP[0],
                        "IEEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].IE[0],
                        "CRTEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].CRT[0], //quando = 1 é do SN
                        "nNF": json.nfeProc.NFe[0].infNFe[0].ide[0].nNF[0],
                        "serie": json.nfeProc.NFe[0].infNFe[0].ide[0].serie[0],
                        "dhSaiEnt": json.nfeProc.NFe[0].infNFe[0].ide[0].dhEmi[0],
                        "UfDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].UF[0],

                        "CPFDest": json.nfeProc.NFe[0].infNFe[0].dest[0].CPF[0],
                        "xNomeDest": json.nfeProc.NFe[0].infNFe[0].dest[0].xNome[0],
                        "nroDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].nro[0],
                        "xBairroDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xBairro[0],
                        "xLgrDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xLgr[0],
                        "cMunDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].cMun[0],
                        "xMunDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xMun[0],
                        "CEPDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].CEP[0],

                        "CNPJTransp": CNPJTransp,
                        "xNomeTransp": xNomeTransp,
                        "IETransp": IETransp,
                        "UFTransp": UFTransp,
                        "xEnderTransp": xEnderTransp,
                        "xMunTransp": xMunTransp,

                        "qVolTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].qVol ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].qVol[0] : null,
                        "pesoLTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoL ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoL[0] : null,
                        "pesoBTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoB ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoB[0] : null,

                        "cProd": product.cProd[0],
                        "orig": product.orig ? product.orig[0] : null,
                        "cEAN": product.cEAN[0],
                        "xProd": product.xProd[0],
                        "CFOP": product.CFOP[0],
                        "vProd": parseFloat(product.vProd[0]),
                        "uTrib": product.uTrib[0],
                        "qTrib": parseFloat(product.qTrib[0]),
                        "vUnTrib": parseFloat(product.vUnTrib[0]),
                        "vFrete": product.vFrete ? parseFloat(product.vFrete[0]) : null,
                        "vBCUFDest": product.vBCUFDest ? parseFloat(product.vBCUFDest[0]) : null,
                        "pFCPUFDest": product.pFCPUFDest ? parseFloat(product.pFCPUFDest[0]) : null,
                        "pICMSUFDest": product.pICMSUFDest ? parseFloat(product.pICMSUFDest[0]) : null,
                        "pICMSInter": product.pICMSInter ? parseFloat(product.pICMSInter[0]) : null,
                        "pICMSInterPart": product.pICMSInterPart ? parseFloat(product.pICMSInterPart[0]) : null,
                        "vFCPUFDest": product.vFCPUFDest ? parseFloat(product.vFCPUFDest[0]) : null,
                        "vICMSUFDest": product.vICMSUFDest ? parseFloat(product.vICMSUFDest[0]) : null,
                        "vICMSUFRemet": product.vICMSUFRemet ? parseFloat(product.vICMSUFRemet[0]) : null,
                        "NCM": product.NCM[0],

                        "vNF": json.nfeProc.NFe[0].infNFe[0].total[0].ICMSTot[0].vNF[0]
                    }
                    jsons.push(json_aux)
                }
            } catch (error) {
                console.log("erro: ", files[i], error.message)
            }
        }
        return jsons;
    }

    async function xmlsInCSV(mainPath, fileType = ".xml", level = 1) {
        let paths = await listFilesInDiretory(mainPath);
        let files = paths.filter(elem => elem.split('/').length < level).filter(elem => elem.includes(fileType))
        var jsons = [];
        for (let i = 0; i < files.length; i++) {
            try {
                const json = await xmlToJson(files[i]);
                const transportadora = json.nfeProc.NFe[0].infNFe[0].transp[0].transporta;
                let CNPJTransp = '';
                let xNomeTransp = '';
                let IETransp = '';
                let UFTransp = '';
                let xEnderTransp = '';
                let xMunTransp = '';
                if (transportadora) {
                    CNPJTransp = transportadora[0].CNPJ ? transportadora[0].CNPJ[0] : null;
                    xNomeTransp = transportadora[0].xNome ? transportadora[0].xNome[0] : null;
                    IETransp = transportadora[0].IE ? transportadora[0].IE[0] : null;
                    UFTransp = transportadora[0].UF ? transportadora[0].UF[0] : null;
                    xEnderTransp = transportadora[0].xEnder ? transportadora[0].xEnder[0] : null;
                    xMunTransp = transportadora[0].xMun ? transportadora[0].xMun[0] : null;
                }
                const json_aux = {
                    "CNPJEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].CNPJ[0],
                    "xLgr": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].xLgr[0],
                    "xNomeEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].xNome[0],
                    "nroEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].nro[0],
                    "xMunEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].xMun[0],
                    "UFEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].UF[0],
                    "CEPEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].enderEmit[0].CEP[0],
                    "IEEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].IE[0],
                    "CRTEmit": json.nfeProc.NFe[0].infNFe[0].emit[0].CRT[0], //quando = 1 é do SN
                    "nNF": json.nfeProc.NFe[0].infNFe[0].ide[0].nNF[0],
                    "serie": json.nfeProc.NFe[0].infNFe[0].ide[0].serie[0],
                    "dhSaiEnt": json.nfeProc.NFe[0].infNFe[0].ide[0].dhEmi[0],
                    "UfDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].UF[0],

                    "CPFDest": json.nfeProc.NFe[0].infNFe[0].dest[0].CPF[0],
                    "xNomeDest": json.nfeProc.NFe[0].infNFe[0].dest[0].xNome[0],
                    "nroDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].nro[0],
                    "xLgrDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xLgr[0],
                    "xBairroDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xBairro[0],
                    "cMunDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].cMun[0],
                    "xMunDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].xMun[0],
                    "CEPDest": json.nfeProc.NFe[0].infNFe[0].dest[0].enderDest[0].CEP[0],

                    "CNPJTransp": CNPJTransp,
                    "xNomeTransp": xNomeTransp,
                    "IETransp": IETransp,
                    "UFTransp": UFTransp,
                    "xEnderTransp": xEnderTransp,
                    "xMunTransp": xMunTransp,

                    "qVolTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].qVol ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].qVol[0] : null,
                    "pesoLTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoL ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoL[0] : null,
                    "pesoBTransp": json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoB ? json.nfeProc.NFe[0].infNFe[0].transp[0].vol[0].pesoB[0] : null,

                    "vNF": json.nfeProc.NFe[0].infNFe[0].total[0].ICMSTot[0].vNF[0]
                }
                jsons.push(json_aux)
            } catch (error) {
                console.log("erro: ", files[i], error.message)
            }
        }
        return jsons;
    }

    async function json2CsvFile(json, path) {
        converter.json2csv(json, async (err, csv) => {
            if (err) {
                throw err;
            }
            await save(csv, path + '.csv');
        });
    }

    function includeEspacosVazios(valorIn, size) {
        let valor = valorIn + "";
        let str = "";
        for (let i = 0; i < size - valor.length; i++) {
            str = str + " ";
        }
        return valor + str;
    }

    function formatData(date) {
        return date.substring(8, 10) + date.substring(5, 7) + date.substring(2, 4)
    }
    function formatDataFull(date) {
        return date.substring(8, 10) + date.substring(5, 7) + date.substring(0, 4)
    }
    function formatHora(date) {
        return date.substring(11, 13) + date.substring(14, 16)
    }

    function arrSum(arr) {
        let soma = 0;
        for (const iterator of arr) {
            soma += parseFloat(iterator);
        }
        return soma;
    }

    function objectValuesToString(object, str) {
        for (const value of Object.keys(object)) {
            str += object[value]
        }
        return str;

    }

    async function generateNOTFIS(jsons, CNPJTransp, CNPJEmit) {
        const dataNow = new Date();
        const horaMinuto = dataNow.getHours().toString() + dataNow.getMinutes().toString();
        const notasFiltradas = jsons.filter(item => item.CNPJTransp == CNPJTransp && item.CNPJEmit == CNPJEmit)
        const notas = notasFiltradas.filter((value, index, self) => self.map(e => e.nNF).indexOf(value.nNF) === index)
        const transpName = notas.find(elem => elem.CNPJTransp == CNPJTransp)?.xNomeTransp;
        const emitName = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.xNomeEmit;
        const emitIE = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.IEEmit
        const emitEndereco = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.xLgr
        const emitCidade = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.xMunEmit
        const emitPostalCode = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.CEPEmit
        const emitUF = notas.find(elem => elem.CNPJEmit == CNPJEmit)?.UFEmit
        let notfis = "";
        let unb = {
            identicador_de_registro: "000",
            identificacao_do_remetente: includeEspacosVazios(emitName, 35),
            identificacao_do_destinatario: includeEspacosVazios(transpName, 39),
            data: includeEspacosVazios(formatData(dataNow.toISOString()), 6),
            hora: includeEspacosVazios(horaMinuto, 4),
            identicador_do_intercambio: includeEspacosVazios(dataNow.getTime().toString().substring(0, 12), 12),
            filler: includeEspacosVazios(" ", 145)
        }
        notfis = objectValuesToString(unb, notfis) + "\r\n";
        let unh = {
            identicador_de_registro: "310",
            identificacao_do_documento: includeEspacosVazios("NOT" + dataNow.getTime().toString().substring(0, 12), 14),
            filler: includeEspacosVazios(" ", 223)
        }
        notfis = objectValuesToString(unh, notfis) + "\r\n";
        let dem = {
            identicador_de_registro: "311",
            cgc: includeEspacosVazios(CNPJEmit, 14),
            incricao_estadual_embarcadora: includeEspacosVazios(emitIE, 15),
            endereco: includeEspacosVazios(emitEndereco, 40),
            cidade: includeEspacosVazios(emitCidade, 35),
            codigo_postal: includeEspacosVazios(emitPostalCode, 9),
            subentidade_de_pais: includeEspacosVazios(emitUF, 9),
            data_embarque_mercadoria: includeEspacosVazios(formatDataFull(dataNow.toISOString()), 8),
            nome_empresa_embarcadora: includeEspacosVazios(transpName, 40),
            filler: includeEspacosVazios(" ", 67)
        }
        notfis = objectValuesToString(dem, notfis) + "\r\n";
        const des_dfn = notas.map(nt => {
            return {
                identicador_de_registro_des: "312",
                razao_social: includeEspacosVazios(nt.xNomeDest, 40),
                cgc_cpf: includeEspacosVazios(nt.CPFDest, 14),
                inscricao_estadual: includeEspacosVazios("", 15),
                endereco: includeEspacosVazios(nt.xLgrDest + ", " + nt.nroDest, 40),
                bairro: includeEspacosVazios(nt.xBairroDest, 20),
                cidade: includeEspacosVazios(nt.xMunDest, 35),
                codigo_postal: includeEspacosVazios(nt.CEPDest, 9),
                codigo_municipio: includeEspacosVazios(nt.cMunDest, 9),
                subentidade_de_pais: includeEspacosVazios(nt.UfDest, 9),
                area_de_frete: includeEspacosVazios("", 4),
                numero_de_comunicacao: includeEspacosVazios("", 35),
                tipo_de_identificacao_destinatario: includeEspacosVazios("2", 1),
                filler: includeEspacosVazios("", 6),
                quebra: "\r\n",
                identicador_de_registro_dfn: "313",
                num_romaneio: includeEspacosVazios(nt.nNF, 15),
                codigo_rota: includeEspacosVazios("", 7),
                meio_transporte: includeEspacosVazios("", 1),
                tipo_transporte_da_carga: includeEspacosVazios("", 1),
                tipo_de_carga: includeEspacosVazios("C", 1),
                condicao_de_frete: includeEspacosVazios("", 1),
                serie_da_nota_fiscal: includeEspacosVazios(nt.serie, 3),
                numero_da_nota_fiscal: includeEspacosVazios(nt.nNF, 8),
                data_emissao: includeEspacosVazios(formatDataFull(nt.dhSaiEnt), 8),
                natureza_da_mercadoria: includeEspacosVazios("", 15),
                especie_de_acondicionamento: includeEspacosVazios("VOLUMES", 15),
                qtde_volumes: includeEspacosVazios(notas.length, 7),
                valor_total_nota: includeEspacosVazios(nt.vNF, 15),
                peso_total_mercadoria: includeEspacosVazios(nt.pesoLTransp, 7),
                peso_densidade_cubagem: includeEspacosVazios(nt.pesoBTransp, 5)
            }
        })
        for (const iterator of des_dfn) {
            notfis = objectValuesToString(iterator, notfis) + "\r\n";
        }
        let tot = {
            identicador_de_registro: "318",
            valor_total_das_notas_fiscais: includeEspacosVazios(arrSum(notas.map(e => e.vNF)), 15),
            peso_total_das_notas_fiscais: includeEspacosVazios(arrSum(notas.map(e => e.pesoLTransp)), 15),
            peso_total_densidade_cubagem: includeEspacosVazios(arrSum(notas.map(e => e.pesoBTransp)), 15),
            quantidade_total_volumes: includeEspacosVazios(notas.length, 15),
            valor_total_a_ser_cobrado: includeEspacosVazios("0", 15),
            valor_total_do_seguro: includeEspacosVazios("0", 15),
            filler: includeEspacosVazios(" ", 147)
        }
        notfis = objectValuesToString(tot, notfis) + "\r\n";
        return [notfis, dataNow.getTime().toString().substring(0, 12)];
    }

    return {
        generateNOTFIS,
        listFilesInDiretory,
        separateNfeXmlFilesByRegion,
        xmlsInCSV,
        save,
        json2CsvFile
    }
}
module.exports = create()
