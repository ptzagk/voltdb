var latencyDetails = [];
function loadAnalysisPage(){
    $("#tabProcedureBtn").trigger("click");

    $("#tabAnalysis li a").on("click", function(){
        setInterval(function(){
            window.dispatchEvent(new Event('resize'));
        },200)

    })

    $("#ulProcedure li a").on("click", function(){
        if($($(this)[0]).text() == "Frequency"){
            $("#spanAnalysisLegend").html("Frequency");
        } else if($($(this)[0]).text() == "Combined"){
            $("#spanAnalysisLegend").html("Combined");
        } else {
            $("#spanAnalysisLegend").html("Execution Time");
        }
        setInterval(function(){
            window.dispatchEvent(new Event('resize'));
        },200)

    })

    function calculateCombinedValue(profileData){
        var totalValue = 0;
        for(var j = 0; j < profileData.length; j++){
            totalValue += (profileData[j].AVG/100000000) * profileData[j].INVOCATIONS;
        }
        return totalValue;
    }

    function fetchData (){
        voltDbRenderer.GetProcedureProfileInformation(function(profileData){
            if(profileData != undefined){
                if(!$.isEmptyObject(profileData["PROCEDURE_PROFILE"])){
                    $(".analyzeNowContent").hide();
                    $(".dataContent").show();
                    $(".noDataContent").hide();
                } else {
                    $(".mainContentAnalysis").hide();
                    $(".dataContent").hide();
                    $(".noDataContent").show();

                }
                $("#tblAnalyzeNowContent").hide();
                $("#tblNoDataContent").show();

            }

            //order the procedure by  their (avg_exec_time * #of invocation) value
            profileData["PROCEDURE_PROFILE"].sort(function(a,b) {return ((b.AVG * b.INVOCATIONS) > (a.AVG * a.INVOCATIONS)) ? 1 : (((a.AVG * a.INVOCATIONS) > (b.AVG * b.INVOCATIONS)) ? -1 : 0);} );

            var dataLatency = [];
            var dataFrequency = [];
            var dataCombined = [];
            var sumOfAllProcedure = calculateCombinedValue(profileData["PROCEDURE_PROFILE"])
            for(var i = 0; i < profileData["PROCEDURE_PROFILE"].length; i++){
                var combinedWeight = (profileData["PROCEDURE_PROFILE"][i].AVG/100000000 * profileData["PROCEDURE_PROFILE"][i].INVOCATIONS)/sumOfAllProcedure;

                VoltDbAnalysis.procedureValue[profileData["PROCEDURE_PROFILE"][i].PROCEDURE] = {AVG: profileData["PROCEDURE_PROFILE"][i].AVG/100000000,
                INVOCATIONS: profileData["PROCEDURE_PROFILE"][i].INVOCATIONS, COMBINED: combinedWeight}

                dataLatency.push({"label": profileData["PROCEDURE_PROFILE"][i].PROCEDURE , "value": profileData["PROCEDURE_PROFILE"][i].AVG/100000000})

                dataFrequency.push({"label": profileData["PROCEDURE_PROFILE"][i].PROCEDURE, "value": profileData["PROCEDURE_PROFILE"][i].INVOCATIONS})
                dataCombined.push({"label": profileData["PROCEDURE_PROFILE"][i].PROCEDURE, "value": combinedWeight})
            }

            MonitorGraphUI.initializeAnalysisGraph();
            MonitorGraphUI.RefreshAnalysisLatencyGraph(dataLatency);
            MonitorGraphUI.RefreshAnalysisFrequencyGraph(dataFrequency);
            MonitorGraphUI.RefreshAnalysisCombinedGraph(dataCombined);
        })

        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            var latencyDetails = [];

            procedureDetails["PROCEDURE_DETAIL"].sort(function(a, b) {
                return parseFloat(b.AVG_EXECUTION_TIME) - parseFloat(a.AVG_EXECUTION_TIME);
            });
            procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                VoltDbAnalysis.latencyDetailValue.push({"label": item.STATEMENT + '(' + item.PARTITION_ID + ')' , "value": item.AVG_EXECUTION_TIME, "PROCEDURE": item.PROCEDURE})
            });
            MonitorGraphUI.initializeProcedureDetailGraph();
        });
    }

    $("#btnAnalyzeNow").on("click", function(){
        fetchData();
    })
}

(function(window) {
    iVoltDbAnalysis = (function(){
        this.procedureValue = {};
        this.latencyDetailValue = [];
    });
    window.VoltDbAnalysis = new iVoltDbAnalysis();
})(window);