#!/usr/bin/env python3
"""
Account Publisher Script

Publishes randomly generated Account records to Salesforce every minute using the
existing Salesforce Pub/Sub API infrastructure.

Usage:
    python account_publisher.py

Environment Variables Required:
    SALESFORCE_USERNAME - Salesforce username
    SALESFORCE_PASSWORD - Salesforce password + security token
    SALESFORCE_INSTANCE_URL - Salesforce instance URL
    GRPC_HOST - Salesforce gRPC host (default: api.pubsub.salesforce.com)
    GRPC_PORT - Salesforce gRPC port (default: 7443)
    API_VERSION - Salesforce API version (default: 57.0)
"""

import logging
import os
import random
import time
from typing import Any, Dict

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Suppress INFO logs from grpc and other verbose libraries
logging.getLogger("grpc").setLevel(logging.WARNING)


class AccountDataGenerator:
    """Generates random Account data for publishing to Salesforce."""

    # Sample data for generating realistic Account records
    COMPANY_PREFIXES = [
        "Global",
        "Advanced",
        "Premier",
        "Elite",
        "Dynamic",
        "Innovative",
        "Strategic",
        "Optimal",
        "Universal",
        "Progressive",
        "Excellence",
        "Superior",
        "Prime",
    ]

    COMPANY_SUFFIXES = [
        "Solutions",
        "Technologies",
        "Systems",
        "Enterprises",
        "Corp",
        "Inc",
        "LLC",
        "Group",
        "Associates",
        "Partners",
        "Consulting",
        "Services",
        "Industries",
    ]

    STREET_NAMES = [
        "Main St",
        "Oak Ave",
        "Park Rd",
        "First St",
        "Second Ave",
        "Elm St",
        "Pine Ave",
        "Market St",
        "Church St",
        "High St",
        "School Ave",
        "Center St",
        "North St",
    ]

    # City-State mappings to ensure realistic combinations
    CITY_STATE_MAPPINGS = [
        ("New York", "NY"),
        ("Los Angeles", "CA"),
        ("Chicago", "IL"),
        ("Houston", "TX"),
        ("Phoenix", "AZ"),
        ("Philadelphia", "PA"),
        ("San Antonio", "TX"),
        ("San Diego", "CA"),
        ("Dallas", "TX"),
        ("San Jose", "CA"),
        ("Austin", "TX"),
        ("Jacksonville", "FL"),
        ("Fort Worth", "TX"),
        ("Columbus", "OH"),
        ("Charlotte", "NC"),
        ("Seattle", "WA"),
        ("Denver", "CO"),
        ("Detroit", "MI"),
        ("Boston", "MA"),
        ("Memphis", "TN"),
        ("Nashville", "TN"),
        ("Baltimore", "MD"),
        ("Portland", "OR"),
        ("Las Vegas", "NV"),
    ]

    @staticmethod
    def generate_company_name() -> str:
        """Generate a realistic company name."""
        prefix = random.choice(AccountDataGenerator.COMPANY_PREFIXES)
        suffix = random.choice(AccountDataGenerator.COMPANY_SUFFIXES)
        return f"{prefix} {suffix}"

    @staticmethod
    def generate_phone() -> str:
        """Generate a realistic US phone number."""
        area_code = random.randint(200, 999)
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"

    @staticmethod
    def generate_billing_address() -> Dict[str, str]:
        """Generate a realistic billing address with proper city-state mapping."""
        street_number = random.randint(100, 9999)
        street_name = random.choice(AccountDataGenerator.STREET_NAMES)
        city, state = random.choice(AccountDataGenerator.CITY_STATE_MAPPINGS)
        postal_code = random.randint(10000, 99999)

        return {
            "BillingStreet": f"{street_number} {street_name}",
            "BillingCity": city,
            "BillingState": state,
            "BillingPostalCode": str(postal_code),
            "BillingCountry": "United States",
        }

    @staticmethod
    def generate_account() -> Dict[str, Any]:
        """Generate a simple Account record with just Name and Phone."""
        account_data = {
            "Name": AccountDataGenerator.generate_company_name(),
            "Phone": AccountDataGenerator.generate_phone(),
        }
        return account_data


class AccountPublisher:
    """Creates Account records in Salesforce using the REST API, which triggers AccountChangeEvent automatically."""

    def __init__(self):
        """Initialize the AccountPublisher with Salesforce connection."""
        self.logger = logging.getLogger(__name__)

        # Required configuration
        self.instance_url = os.getenv("SALESFORCE_INSTANCE_URL")
        self.username = os.getenv("SALESFORCE_USERNAME")
        self.password = os.getenv("SALESFORCE_PASSWORD")
        self.api_version = os.getenv("API_VERSION", "57.0")

        # Validate required configuration
        required_vars = ["instance_url", "username", "password"]
        missing_vars = [var for var in required_vars if not getattr(self, var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        # Initialize session token and headers
        self.session_token = None
        self.headers = None

        # Authenticate and get session token
        self.logger.info("Authenticating with Salesforce...")
        self._authenticate()
        self.logger.info("Authentication successful!")

    def _authenticate(self):
        """Authenticate with Salesforce using SOAP login to get session token."""
        import xml.etree.ElementTree as et

        import requests

        # SOAP login request
        url_suffix = f"/services/Soap/u/{self.api_version}/"
        headers = {"content-type": "text/xml", "SOAPAction": "Login"}
        xml_body = (
            "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
            + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
            + "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>"
            + "<urn:login><urn:username><![CDATA["
            + self.username
            + "]]></urn:username><urn:password><![CDATA["
            + self.password
            + "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
        )

        response = requests.post(
            self.instance_url + url_suffix, data=xml_body, headers=headers
        )

        if response.status_code != 200:
            raise Exception(
                f"SOAP authentication failed with status {response.status_code}: {response.text}"
            )

        # Parse response to get session token
        res_xml = et.fromstring(response.content.decode("utf-8"))[0][0][0]
        self.session_token = res_xml[4].text

        # Set up REST API headers
        self.headers = {
            "Authorization": f"Bearer {self.session_token}",
            "Content-Type": "application/json",
        }

    def create_account(self, account_data: Dict[str, Any]) -> bool:
        """Create a single Account record in Salesforce using REST API."""
        import requests

        try:
            # REST API endpoint for creating Account
            url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects/Account/"

            # Log the account we're creating
            self.logger.info(f"Creating Account: {account_data['Name']}")

            # Make the REST API call
            response = requests.post(url, json=account_data, headers=self.headers)

            # Check response
            if response.status_code == 201:
                result = response.json()
                account_id = result.get("id", "Unknown")
                self.logger.info(f"✅ Account created successfully! ID: {account_id}")
                return True
            else:
                error_details = response.text
                self.logger.error(
                    f"❌ Failed to create Account: {response.status_code} - {error_details}"
                )
                return False

        except Exception as e:
            self.logger.error(f"❌ Error creating account: {e}")
            return False

    def start_publishing(self, interval_seconds: int = 60):
        """Start publishing Account records at regular intervals."""
        self.logger.info(
            f"🚀 Starting Account publisher - creating new Account every {interval_seconds} seconds"
        )

        publish_count = 0
        success_count = 0

        try:
            while True:
                # Generate random account data
                account_data = AccountDataGenerator.generate_account()

                # Create the account in Salesforce (this will trigger AccountChangeEvent automatically)
                success = self.create_account(account_data)

                # Update counters
                publish_count += 1
                if success:
                    success_count += 1

                # Log progress every 10 attempts
                if publish_count % 10 == 0:
                    success_rate = (success_count / publish_count) * 100
                    self.logger.info(
                        f"📊 Progress: {publish_count} attempts, {success_count} successful ({success_rate:.1f}%)"
                    )

                # Wait for next interval
                self.logger.info(
                    f"⏱️  Waiting {interval_seconds} seconds until next Account..."
                )
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            self.logger.info(
                f"\n🛑 Publisher stopped by user. Final stats: {success_count}/{publish_count} successful"
            )
        except Exception as e:
            self.logger.error(f"❌ Publisher error: {e}")
            raise


def main():
    """Main entry point for the Account publisher."""
    print("🏢 Salesforce Account Publisher")
    print("=" * 40)

    try:
        # Create and start publisher
        publisher = AccountPublisher()
        publisher.start_publishing(interval_seconds=3)

    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        logging.error(f"Failed to start publisher: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
