    function showOptimizationStatus(heading, message, showApiKeyInput = false) {
        document.getElementById('optimization-loading').classList.add('d-none');
        document.getElementById('optimization-status-container').classList.remove('d-none');
        document.getElementById('optimization-status-heading').textContent = heading;
        document.getElementById('optimization-status-message').textContent = message;
        
        // Show or hide API key input
        const apiKeyInputContainer = document.getElementById('api-key-input-container');
        if (showApiKeyInput) {
            apiKeyInputContainer.classList.remove('d-none');
        } else {
            apiKeyInputContainer.classList.add('d-none');
        }
    }
