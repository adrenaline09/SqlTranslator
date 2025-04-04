                if (!statusData.available) {
                    showOptimizationStatus('Feature Not Available', statusData.message || 'AI optimization requires an OpenAI API key', true);
                    return;
                }
