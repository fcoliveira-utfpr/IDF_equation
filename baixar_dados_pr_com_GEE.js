// Inicializa o Earth Engine
var colecaoPath = "projects/sat-io/open-datasets/BR-DWGD/PR";
var banda = "b1";
var offset = 225;
var escala = 0.006866665;

// Lista de anos
var anos = ee.List.sequence(1991, 2021);

// Importar o conjunto de dados GADM do Brasil
var gadm = ee.FeatureCollection("projects/fcoliveira/assets/gadm41_BRA_2");

// Filtrar os municípios do Paraná
var municipiosParana = gadm.filter(ee.Filter.eq('NAME_1', 'Paraná'));

// Função para calcular o centroide e criar uma feature com o nome do município
var calcularCentroide = function(feature) {
  var centroide = feature.geometry().centroid();
  var nomeMunicipio = feature.get('NAME_2');
  return ee.Feature(centroide, {Municipio: nomeMunicipio});
};

// Aplicar a função a cada município do Paraná
var centroidesMunicipios = municipiosParana.map(calcularCentroide);

// Usar a coleção de centroides como lista de municípios
var municipios = centroidesMunicipios;

// Função para processar os dados
function processarAno(ano) {
    var dataInicial = ee.Date.fromYMD(ano, 1, 1);
    var dataFinal = ee.Date.fromYMD(ano, 12, 31);
  
    return municipios.map(function(municipio) {
        var nomeMunicipio = ee.String(municipio.get("Municipio"));
        var ponto = municipio.geometry();
        
        var colecao = ee.ImageCollection(colecaoPath)
            .filterDate(dataInicial, dataFinal)
            .select([banda])
            .filterBounds(ponto);
        
        var dados = colecao.map(function(img) {
            var valor = img.reduceRegion({
                reducer: ee.Reducer.mean(),
                geometry: ponto,
                scale: 10
            }).get(banda);

            return ee.Feature(ponto, {
                "Municipio": nomeMunicipio,
                "Ano": ano,
                "Data": img.date().format("YYYY-MM-dd"),
                "Chuva_mm": ee.Number(valor).multiply(escala).add(offset).max(0) // Ajuste dos valores
            });
        });

        return dados;
    }).flatten();
}

// Aplicar a função para todos os anos e combinar resultados
var dadosFinais = ee.FeatureCollection(anos.map(processarAno)).flatten();

// Exportar para Google Drive
Export.table.toDrive({
    collection: dadosFinais,
    description: "Precipitacao_Municipios_PR",
    fileFormat: "CSV"
});

print("Processamento concluído! Verifique o Google Drive para baixar o CSV.");
