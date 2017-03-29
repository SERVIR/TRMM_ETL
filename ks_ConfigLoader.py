#-------------------------------------------------------------------------------
# Name:        ks_ConfigLoader.py
# Purpose:     Configuration Loader (So we can use an XML file to load our
#               rather than hard coding them into the script files.
#
# Author:      Kris Stanton
#
# Created:     01/27/2014 (mm/dd/yyyy)
# Copyright:   (c) kstanto1 2014
# Licence:     <your licence>
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------

#import xml.etree.cElementTree as et


# http://code.activestate.com/recipes/410469-xml-as-dictionary/  # START
import xml.etree.cElementTree as ElementTree

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    '''
    Example usage:

    >>> tree = ElementTree.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlDictConfig(root)

    Or, if you want to use an XML string:

    >>> root = ElementTree.XML(xml_string)
    >>> xmldict = XmlDictConfig(root)

    And then use xmldict for what it is... a dict.
    '''
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})

# http://code.activestate.com/recipes/410469-xml-as-dictionary/  # END



class ks_ConfigLoader(object):
    '''
        ks_ConfigLoader.path      Path to config file (if None, than uses current folder this file is in)
        ks_ConfigLoader.tree      ElementTree object containing config xml file contents.
        ks_ConfigLoader.xmldict   Dictionary object of entire xml structure (starting with node 'GlobalSettings')
        ks_ConfigLoader.__init__  class constructor
    '''
    #def __init__(self, pathToConfigFile=None):
    def __init__(self, pathToConfigFile):
        # Set Members
        self.path = pathToConfigFile

        # Store the tree object
        #self.tree = ElementTree.parse('Servir_Generic_ETL_Config.xml')
        #self.tree = ElementTree.parse('config.xml')
        self.tree = ElementTree.parse(self.path)

        # Convert the tree to a dictionary
        root = self.tree.getroot()
        self.xmldict = XmlDictConfig(root)

        # Convert each Setting item from the dictionary into an expected value


        # Garbage/Debug

    # Example function for "getting" a setting item.
    def get_ExampleSettingOne(self):
        #return self.xmldict.GlobalSettings.ExampleSettingOne
        GlobalSettings = self.xmldict['GlobalSettings'] #self.xmldict.get('GlobalSettings') also works..
        return GlobalSettings['ExampleSettingOne']


    def get_GlobalSettings(self):
        GlobalSettings = self.xmldict['GlobalSettings']
        return GlobalSettings

    def get_ETL_Settings(self):
        GlobalSettings = self.get_GlobalSettings()
        return GlobalSettings['ETL_Settings']