"""
AI Enrichment module for company details
"""
import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class AIEnrichment:
    """Handles AI enrichment of company information"""
    
    def __init__(self):
        self.provider = os.getenv('AI_PROVIDER', 'openai').lower()
        self.setup_client()
    
    def setup_client(self):
        """Initialize AI client based on provider"""
        if self.provider == 'openai':
            try:
                import openai
                
                # Check for OpenRouter configuration first
                openrouter_key = os.getenv('OPENROUTER_API_KEY')
                if openrouter_key:
                    self.client = openai.OpenAI(
                        api_key=openrouter_key,
                        base_url="https://openrouter.ai/api/v1"
                    )
                    self.model = os.getenv('OPENROUTER_MODEL', 'deepseek/deepseek-chat-v3.1:free')
                    logger.info(f"Using OpenRouter with model: {self.model}")
                else:
                    # Standard OpenAI
                    api_key = os.getenv('OPENAI_API_KEY')
                    if not api_key:
                        logger.warning("OPENAI_API_KEY not set, AI enrichment disabled")
                        self.client = None
                        return
                    self.client = openai.OpenAI(api_key=api_key)
                    self.model = 'gpt-3.5-turbo'
                    
            except ImportError:
                logger.error("OpenAI package not installed")
                self.client = None
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI client: {e}")
                self.client = None
        elif self.provider == 'anthropic':
            try:
                import anthropic
                api_key = os.getenv('ANTHROPIC_API_KEY')
                if not api_key:
                    logger.warning("ANTHROPIC_API_KEY not set, AI enrichment disabled")
                    self.client = None
                    return
                self.client = anthropic.Anthropic(api_key=api_key)
                self.model = 'claude-3-haiku-20240307'
            except ImportError:
                logger.error("Anthropic package not installed")
                self.client = None
            except Exception as e:
                logger.error(f"Failed to initialize Anthropic client: {e}")
                self.client = None
        else:
            logger.error(f"Unsupported AI provider: {self.provider}")
            self.client = None
    
    def enrich_company(self, company_name: str, company_type: str) -> Dict[str, Any]:
        """
        Enrich company information using AI
        
        Args:
            company_name: Name of the company
            company_type: Type of company (Manufacturer or Reselling Firm)
        
        Returns:
            Dictionary with enriched company details
        """
        if not self.client:
            logger.warning("AI client not available, returning empty enrichment")
            return {
                'founding_date': None,
                'promoter_founder_name': None,
                'company_brief': None,
                'country_code': None
            }
        
        prompt = self._create_enrichment_prompt(company_name, company_type)
        
        try:
            if self.provider == 'openai':
                response = self._call_openai(prompt)
            elif self.provider == 'anthropic':
                response = self._call_anthropic(prompt)
            else:
                return self._empty_enrichment()
            
            return self._parse_ai_response(response)
            
        except Exception as e:
            logger.error(f"AI enrichment failed for {company_name}: {e}")
            return self._empty_enrichment()
    
    def _create_enrichment_prompt(self, company_name: str, company_type: str) -> str:
        """Create the AI prompt for company enrichment"""
        return f"""You are given a company name and type (Manufacturer or Reselling Firm).  
Task: Provide structured metadata in JSON with the following fields:  
- founding_date (YYYY-MM-DD or just YYYY if only year is known, or null)  
- promoter_founder_name (string or null)  
- company_brief (50–100 words, plain text summary of what the company does, location, industry, reputation, or null)
- country_code (ISO 2-letter country code where the company is based, or null)

If nothing is found or you're not confident about the information, return null values.  
Only return the JSON object, no additional text.

Company: {company_name}  
Type: {company_type}  

Example response:
{{
    "founding_date": "1985",
    "promoter_founder_name": "John Smith",
    "company_brief": "Leading pharmaceutical manufacturer based in Ghana, specializing in generic medications and health supplements for the West African market.",
    "country_code": "GH"
}}"""
    
    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API"""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1
        )
        return response.choices[0].message.content.strip()
    
    def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic API"""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=300,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    
    def _parse_ai_response(self, response: str) -> Dict[str, Any]:
        """Parse AI response and extract structured data"""
        try:
            # Clean up response to extract JSON
            response = response.strip()
            
            # Find JSON in response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                
                # Validate and clean data
                result = {
                    'founding_date': self._parse_date(data.get('founding_date')),
                    'promoter_founder_name': self._clean_string(data.get('promoter_founder_name')),
                    'company_brief': self._clean_string(data.get('company_brief')),
                    'country_code': self._clean_country_code(data.get('country_code'))
                }
                
                return result
            else:
                logger.warning("No JSON found in AI response")
                return self._empty_enrichment()
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            logger.error(f"Response was: {response}")
            return self._empty_enrichment()
    
    def _parse_date(self, date_str: Any) -> Optional[str]:
        """Parse and validate date string"""
        if not date_str or date_str in ['null', None]:
            return None
        
        date_str = str(date_str).strip()
        
        # Try different date formats
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y']
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Return in database format
                if fmt == '%Y':
                    return f"{parsed_date.year}-01-01"
                else:
                    return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If it's just a year
        if date_str.isdigit() and len(date_str) == 4:
            year = int(date_str)
            if 1800 <= year <= datetime.now().year:
                return f"{year}-01-01"
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def _clean_string(self, value: Any) -> Optional[str]:
        """Clean and validate string values"""
        if not value or value in ['null', None, 'NULL']:
            return None
        
        cleaned = str(value).strip()
        return cleaned if cleaned and cleaned.lower() != 'null' else None
    
    def _clean_country_code(self, code: Any) -> Optional[str]:
        """Clean and validate country code"""
        if not code or code in ['null', None]:
            return None
        
        code = str(code).strip().upper()
        
        # Basic validation - should be 2 characters
        if len(code) == 2 and code.isalpha():
            return code
        
        return None
    
    def _empty_enrichment(self) -> Dict[str, Any]:
        """Return empty enrichment data"""
        return {
            'founding_date': None,
            'promoter_founder_name': None,
            'company_brief': None,
            'country_code': None
        }

# Global AI enrichment instance
ai_enrichment = AIEnrichment()
