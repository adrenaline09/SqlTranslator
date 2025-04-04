
    // API Key submission handler
    document.getElementById('submit-api-key-btn').addEventListener('click', async function() {
        handleApiKeySubmission();
    });
    
    document.getElementById('openai-api-key').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            handleApiKeySubmission();
        }
    });
    
    async function handleApiKeySubmission() {
        // Get the API key
        const apiKey = document.getElementById('openai-api-key').value.trim();
        
        if (!apiKey) {
            alert('Please enter a valid API key');
            return;
        }
        
        // Show loading
        document.getElementById('optimization-loading').classList.remove('d-none');
        document.getElementById('optimization-status-container').classList.add('d-none');
        
        try {
            // First check if the API key is valid
            const statusResponse = await fetch('/api/optimization-status', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    api_key: apiKey
                })
            });
            
            const statusData = await statusResponse.json();
            
            if (!statusResponse.ok) {
                showOptimizationStatus('Error', statusData.message || 'An error occurred while checking API key');
                return;
            }
            
            if (!statusData.available) {
                showOptimizationStatus('Invalid API Key', statusData.message, true);
                return;
            }
            
            // If valid, retry optimization with the provided API key
            const sql = resultEditor ? resultEditor.getValue() : document.getElementById('converted-sql').value;
            const targetDialect = document.getElementById('target-dialect').value;
            
            const optimizeResponse = await fetch('/api/optimize-sql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    sql: sql,
                    dialect: targetDialect,
                    api_key: apiKey
                })
            });
            
            const optimizeData = await optimizeResponse.json();
            
            if (!optimizeResponse.ok) {
                showOptimizationStatus('Error', optimizeData.message || 'An error occurred during optimization');
                return;
            }
            
            // Show optimization suggestions
            document.getElementById('optimization-loading').classList.add('d-none');
            document.getElementById('optimization-results-container').classList.remove('d-none');
            
            const suggestionsList = document.getElementById('optimization-suggestions');
            suggestionsList.innerHTML = '';
            
            if (optimizeData.suggestions && optimizeData.suggestions.length > 0) {
                optimizeData.suggestions.forEach(suggestion => {
                    const listItem = document.createElement('li');
                    listItem.className = 'list-group-item bg-dark text-light border-secondary';
                    
                    // Create title with impact badge
                    const titleDiv = document.createElement('div');
                    titleDiv.className = 'd-flex justify-content-between align-items-center';
                    
                    const titleHeading = document.createElement('h5');
                    titleHeading.textContent = suggestion.title;
                    titleHeading.className = 'mb-0';
                    
                    const impactBadge = document.createElement('span');
                    impactBadge.className = 'badge rounded-pill ' + 
                        (suggestion.impact === 'High' ? 'bg-danger' : 
                         suggestion.impact === 'Medium' ? 'bg-warning text-dark' : 'bg-info text-dark');
                    impactBadge.textContent = suggestion.impact;
                    
                    titleDiv.appendChild(titleHeading);
                    titleDiv.appendChild(impactBadge);
                    
                    // Create description
                    const descriptionPara = document.createElement('p');
                    descriptionPara.className = 'mt-2';
                    descriptionPara.textContent = suggestion.description;
                    
                    // Create example if available
                    let exampleDiv = null;
                    if (suggestion.example) {
                        exampleDiv = document.createElement('div');
                        exampleDiv.className = 'mt-2';
                        
                        const exampleTitle = document.createElement('strong');
                        exampleTitle.textContent = 'Example:';
                        
                        const exampleCode = document.createElement('pre');
                        exampleCode.className = 'bg-secondary p-2 rounded mt-1';
                        exampleCode.textContent = suggestion.example;
                        
                        exampleDiv.appendChild(exampleTitle);
                        exampleDiv.appendChild(exampleCode);
                    }
                    
                    // Add all elements to list item
                    listItem.appendChild(titleDiv);
                    listItem.appendChild(descriptionPara);
                    if (exampleDiv) {
                        listItem.appendChild(exampleDiv);
                    }
                    
                    suggestionsList.appendChild(listItem);
                });
            } else {
                const noSuggestionsItem = document.createElement('li');
                noSuggestionsItem.className = 'list-group-item bg-dark text-light border-secondary';
                noSuggestionsItem.textContent = 'No optimization suggestions available for this query.';
                suggestionsList.appendChild(noSuggestionsItem);
            }
        } catch (error) {
            showOptimizationStatus('Error', 'An unexpected error occurred: ' + error.message);
        }
    }
