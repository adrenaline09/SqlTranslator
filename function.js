    function showOptimizationStatus(heading, message, showApiKeyInput = false) {
        document.getElementById('optimization-loading').classList.add('d-none');
        document.getElementById('optimization-status-container').classList.remove('d-none');
        document.getElementById('optimization-status-heading').textContent = heading;
        document.getElementById('optimization-status-message').textContent = message;
        
        // Show or hide the API key input
        const apiKeyContainer = document.getElementById('api-key-input-container');
        if (showApiKeyInput) {
            apiKeyContainer.classList.remove('d-none');
        } else {
            apiKeyContainer.classList.add('d-none');
        }
    }
