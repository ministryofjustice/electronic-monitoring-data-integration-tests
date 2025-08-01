@dms
Feature: DMS
    Extraction from RDS to s3
    Background: 
        Given I have an active AWS SSH Tunnel
    
    Scenario: Assert DMS Extraction
        Given I have uploaded the .bak files into s3
        And the test data has been ingested into RDS
        When I await for the for DMS task to be complete
        Then I validate the DMS extraction

