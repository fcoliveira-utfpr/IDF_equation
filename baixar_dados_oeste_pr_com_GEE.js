// Inicializa o Earth Engine
var colecaoPath = "projects/sat-io/open-datasets/BR-DWGD/PR";
var banda = "b1";
var offset = 225;
var escala = 0.006866665;

// Lista de anos
var anos = ee.List.sequence(1991, 2020);

// Municípios do Oeste do Paraná 
var municipiosOesteParana = [
  "Cascavel", "Toledo", "Foz do Iguaçu", "Medianeira",
  "Marechal Cândido Rondon", "Santa Tereza do Oeste",
  "São Miguel do Iguaçu", "Guaíra", "Missal", "Itaipulândia",
  "Santa Helena", "Diamante do Oeste",
  "Matelândia", "Vera Cruz do Oeste", "Nova Santa Rosa"
];

// Carrega os municípios
var municipiosParana = ee.FeatureCollection("FAO/GAUL/2015/level2")
  .filter(ee.Filter.eq('ADM1_NAME', 'Paraná'))
  .filter(ee.Filter.inList('ADM2_NAME', municipiosOesteParana));

// Função principal de processamento
function processarDados() {
  // Cria uma lista plana de todas combinações município-ano
  var combinacoes = ee.List(municipiosParana.aggregate_array('ADM2_NAME'))
    .map(function(municipio) {
      return anos.map(function(ano) {
        return ee.Feature(null, {
          'municipio': municipio,
          'ano': ano
        });
      });
    }).flatten();
  
  // Processa cada combinação
  var resultados = ee.FeatureCollection(combinacoes.map(function(combinacao) {
    combinacao = ee.Feature(combinacao);
    var nomeMunicipio = combinacao.get('municipio');
    var ano = ee.Number(combinacao.get('ano'));
    
    // Encontra o município correspondente
    var municipio = municipiosParana
      .filter(ee.Filter.eq('ADM2_NAME', nomeMunicipio))
      .first();
    
    var ponto = municipio.geometry().centroid();
    
    var dataInicial = ee.Date.fromYMD(ano, 1, 1);
    var dataFinal = ee.Date.fromYMD(ano, 12, 31);
    
    var colecao = ee.ImageCollection(colecaoPath)
      .filterDate(dataInicial, dataFinal)
      .select(banda)
      .filterBounds(ponto);
    
    // Processa as imagens e encontra o máximo
    var maxFeature = colecao.map(function(image) {
      var valor = ee.Number(image.reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: ponto,
        scale: 1000
      }).get(banda));
      
      var chuva = valor.multiply(escala).add(offset).max(0);
      
      return ee.Feature(null, {
        'Municipio': nomeMunicipio,
        'Ano': ano,
        'Data': image.date().format('YYYY-MM-dd'),
        'Chuva_mm': chuva
      });
    })
    .sort('Chuva_mm', false)
    .first();
    
    // Garante que sempre retorne um Feature válido
    return ee.Feature(maxFeature, {
      'Municipio': nomeMunicipio,
      'Ano': ano,
      'Data': ee.Algorithms.If(ee.Feature(maxFeature).get('Data'), maxFeature.get('Data'), 'N/A'),
      'Chuva_mm': ee.Algorithms.If(ee.Feature(maxFeature).get('Chuva_mm'), maxFeature.get('Chuva_mm'), -1)
    });
  }));
  
  return resultados;
}

// Executa o processamento
var dadosFinais = processarDados();

// Exporta os resultados
Export.table.toDrive({
  collection: dadosFinais,
  description: "Precipitacao_Maxima_Anual_Oeste_PR",
  fileFormat: "CSV",
  selectors: ["Municipio", "Ano", "Data", "Chuva_mm"]
});

print("Processamento concluído! Verifique o Google Drive para baixar o CSV.");
