document.getElementById("agency-sf").addEventListener("change",
async function(e){
    let selectedAgency = document.getElementById("agency-sf").value
    
    formData = new FormData()
    formData.append('selected_agency', selectedAgency)
    
    let resp = await fetch(`/smcchecker/getmasterid`,{
        method: 'post',
        body: formData
    });
        
    let data = await resp.json()
    let masterids = data['masterids']

    var contentString = ` <option value="" disabled selected>Select MasterID</option>`
    Array.from(masterids).forEach(item => {
        contentString += `<option value= "${item}"> ${item}</option>`
    })
    document.getElementById('masterid-sf').innerHTML = contentString;
})

document.getElementById("masterid-sf").addEventListener("change", async function(e){
    
    formData = new FormData()
    formData.append('masterid', document.getElementById("masterid-sf").value)
    formData.append('selected_agency', document.getElementById("agency-sf").value)
    
    let resp = await fetch(`/smcchecker/getdownloadlink`,{
        method: 'post',
        body: formData
    });
        
    let data = await resp.json()
    
    let downLoadLinkSites = data['dl_link_sites']
    let downLoadLinkCatchments = data['dl_link_catchments']

    document.getElementById("download-button-sf-sites").setAttribute("onclick", `location.href='${downLoadLinkSites}'` )
    document.getElementById("download-button-sf-catchments").setAttribute("onclick", `location.href='${downLoadLinkCatchments}'` )

})
