
var delineatedYes
var delineatedNo

document.getElementById("check-station-sf").addEventListener("click",
async function(e){
    
    document.getElementById('visual-map-container').classList.add("hidden")
    document.getElementsByClassName("download-button-container")[0].classList.add("hidden")
    document.getElementById('loading-spinner').classList.remove("hidden")

    let inputStations = document.getElementById("input-station-sf").value.split(',').map(item => item.trim()).join(',');
    console.log(inputStations)
    if (inputStations == ''){
        document.getElementById('loading-spinner').classList.add("hidden")
        return alert('Station inputs are empty')
    }

    formData = new FormData()
    formData.append('input_stations', inputStations)
    
    let resp = await fetch(`/smcchecker/checkstationsf`,{
        method: 'post',
        body: formData
    });
        
    let data = await resp.json()

    notInLookUp = data['not_in_lookup']
    delineatedYes = data['delineated_yes']
    delineatedNo = data['delineated_no']
    aliasReport = data['alias_report']
    
    const messageSlot = document.querySelector('#notice-station-sf div[name="message"]');
    messageSlot.innerHTML = ""
    
    if (notInLookUp.length > 0){
        messageSlot.innerHTML += `<b>Validity Check</b>: Stations (${notInLookUp.join(", ")}) are not in the lookup list. 
        Please contact Jeff Brown jeffb@sccwrp.org to add the stationcodes to lookup list<br><br>`
    }
    
    if (aliasReport.length > 0){
        messageSlot.innerHTML += `<b>Alias Check</b>: ${aliasReport}.<br><br>`
    }

    if (delineatedYes.length > 0){
        messageSlot.innerHTML += `<b>Delineation Check</b>: Stations (${delineatedYes.join(", ")}) have been delineated and submitted to the database. 
        You can view and download the shapefiles for these stations by clicking on the Generate Map button below.
        When the map is generated, you can click on the points/polygon to view the stationid. 
        If you believe there is an error with these shapefiles, contact Jeff Brown jeffb@sccwrp.org. <br><br>`
        document.getElementsByClassName("download-button-container")[0].classList.remove("hidden")
        sessionStorage.setItem('stationIds', delineatedYes.map(item => `'${item}'`).join(', '));
        
    }
    
    if (delineatedNo.length > 0){
        messageSlot.innerHTML += `<b>Delineation Check</b>: Stations (${delineatedNo.join(", ")}) have not been delineated. 
        Please go back to the <a href="https://nexus.sccwrp.org/smcchecker/"> SMC Checker</a>, select Submission Type as Shapefile and submit the data.
        `
    }

    document.getElementById('loading-spinner').classList.add("hidden")
    document.getElementById("notice-station-sf").classList.remove("hidden")
    
})

document.getElementById("download-button-sf").addEventListener("click", async function(e){
    document.getElementById('loading-spinner').classList.remove("hidden")
   
    let stationIds = delineatedYes.map(item => `'${item}'`).join(', ');
    
    formData = new FormData()
    formData.append('stationids', stationIds)
    
    fetch(`/smcchecker/getdownloadlink`,{
        method: 'post',
        body: formData
    }).then(response => response.blob())
    .then(blob => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'shapefiles.zip';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        document.getElementById('loading-spinner').classList.add("hidden")
        
    });

})

document.getElementById("show-map-sf").addEventListener("click", async function(e){
    document.getElementById('loading-spinner').classList.remove("hidden")
    document.getElementById('visual-map-container').classList.remove("hidden")
    document.getElementById('visual-map').setAttribute('src',`/smcchecker/map`)
    document.getElementById('loading-spinner').classList.add("hidden")
})