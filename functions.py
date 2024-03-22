import traceback
import os
import shutil


class Functions:
  
  
    # check webdav path 
    def CheckWebdavPath(webdav_all_path, client, logger):
        for webdav_allpath in webdav_all_path:
            if not client.check(webdav_allpath):
               logger.warning("file:function.py, function:CheckWebdavPath, warning:" + webdav_allpath + " does not exist")
               return True
        return False
      
      
    # check local path 
    def ChecklocalPath(local_all_path, logger):
        for local_allpath in local_all_path:
            if not os.path.exists(local_allpath):
               logger.warning("file:function.py, function:ChecklocalPath, warning:" + local_allpath + " does not exist")
               return True
        return False
      
      
    # close all cnxns
    def CloseConnections(cursor, cnxn, session, logger):  
        try:
           cursor.close()  
           cnxn.close()
           session.close()
        except Exception as e:
               logger.error("file:functions.py, function:CloseConnection, error:" + str(e) + " | " + str(traceback))

          
    # update the action column in brandid table
    def Item_BrandUpdate(brand, action, cursor, logger):
        try: 
           cursor.execute("{CALL EDI_Item_BrandUpdate (?,?)}", (brand, action))
           rows_affected = cursor.rowcount
           if rows_affected>0:
              return True
           else:
               return False
        except Exception as e:
               logger.error("file:functions.py, function:Item_BrandUpdate, error:" + str(e) + " | " + str(traceback))
               return False
          

    # get the new name from database  
    def ItemPictureName_Select(oldname, cursor, logger):
        try:
           base = os.path.splitext(oldname)[0]
           cursor.execute("{CALL [dbo].[EDI_ItemPictureName_Select] (?)}", base)
           NewName = cursor.fetchone()
           if not(NewName is None):
              return NewName
           else:
               return None
        except Exception as e:
               logger.error("file:functions.py, function:ItemPictureName_Select, error:" + str(e) + " | " + str(traceback))
               return None 
        
     
    # get rows from EDI.brandID table having a specific action 
    def BrandID_Get(condition, cursor, logger):
        try:
           cursor.execute("{CALL EDI_BrandID_Get (?)}", (condition))
           data=cursor.fetchall()
           if not(data is None):
              return data
           else:
               return None
        except Exception as e:
               logger.error("file:functions.py, function:BrandID_Get, error:" + str(e) + " | " +str(traceback))  
               return None 
        
         
    # insert log in database
    def ItemPicture_Insert_Get(picture, cursor, logger):
        try:
           cursor.execute("{CALL EDI_ItemPicture_Insert_Get (?)}", (picture))
           rows_affected = cursor.rowcount
           if rows_affected>0:
              return True
           else:
               return False
        except Exception as e:
               logger.error("file:functions.py, function:ItemPicture_Insert_Get, error:" + str(e) + " | " + str(traceback))  
               return False 
        

    # create folders on webdav
    async def CreateBrandFolder_remote(client, logger,  webdav_path_upload_picture, cursor):
          BrandID_Get = Functions.BrandID_Get
          Item_BrandUpdate = Functions.Item_BrandUpdate
          brands = BrandID_Get("New", cursor, logger)
          if brands:
             for brand in brands:
                 try: 
                    Item_BrandUpdate(brand[3], "Created", cursor, logger)
                    if not(client.check( webdav_path_upload_picture + brand[3])):
                       client.mkdir( webdav_path_upload_picture + brand[3])
                       cursor.commit()
                       logger.info("file:functions.py, function:CreateBrandFolder_remote, info:folder created-" + str(brand[3]))
                    else:
                        cursor.commit()
                        logger.warning("file:functions.py, function:CreateBrandFolder_remote, warning:folder already exists-" + str(brand[3]))
                 except Exception as e:
                        logger.error("file:functions.py, function:CreateBrandFolder_remote, error:" + str(e) + " | " + str(traceback))
   
   
    # create folders on webdav
    async def CreateBrandFolder_local(client, logger, local_webdav_upload_pictures, cursor):
          BrandID_Get = Functions.BrandID_Get
          Item_BrandUpdate = Functions.Item_BrandUpdate
          brands = BrandID_Get("New", cursor, logger)
          if brands:
             for brand in brands:
                 try: 
                    Item_BrandUpdate(brand[3], "Created", cursor, logger)
                    if not(os.path.exists(local_webdav_upload_pictures + brand[3])):
                       os.mkdir(local_webdav_upload_pictures + brand[3])
                       cursor.commit()
                       logger.info("file:functions.py, function:CreateBrandFolder_local, info:folder created-" + str(brand[3]))
                    else:
                        cursor.commit()
                        logger.warning("file:functions.py, function:CreateBrandFolder_local, warning:folder already exists-" + str(brand[3]))
                 except Exception as e:
                        logger.error("file:functions.py, function:CreateBrandFolder_local, error:" + str(e) + " | " + str(traceback))
   
   
    # update folders on webdav
    async def UpdateBrandFolder_remote(client, logger,  webdav_path_upload_picture, cursor):
          BrandID_Get = Functions.BrandID_Get
          Item_BrandUpdate = Functions.Item_BrandUpdate
          brands = BrandID_Get("Update", cursor, logger)
          if brands:
             for brand in brands:
               try: 
                  Item_BrandUpdate(brand[3], "Updated", cursor, logger)
                  if client.check(webdav_path_upload_picture + brand[4]):
                     client.move(webdav_path_upload_picture + brand[4], webdav_path_upload_picture + brand[3])
                     logger.info("file:functions.py, function:UpdateBrandFolder_remote, info:folder updated from-" + str(brand[4]) + " to " + str(brand[3]))
                     cursor.commit()
                  elif not(client.check(webdav_path_upload_picture + brand[3])):
                       client.mkdir(webdav_path_upload_picture + brand[3])
                       cursor.commit()
                       logger.warning("file:functions.py, function:UpdateBrandFolder_remote, warning:folder created intstead of updating it-" + str(brand[3]))
               except Exception as e:
                      logger.error("file:functions.py, function:UpdateBrandFolder_remote, error:" + str(e) + " | " + str(traceback))


    # update folders on webdav
    async def UpdateBrandFolder_local(client, logger, local_webdav_upload_pictures, cursor):
          BrandID_Get = Functions.BrandID_Get
          Item_BrandUpdate = Functions.Item_BrandUpdate
          brands = BrandID_Get("Update", cursor, logger)
          if brands:
             for brand in brands:
                 try: 
                    Item_BrandUpdate(brand[3], "Updated", cursor, logger)
                    if os.path.exists(local_webdav_upload_pictures + brand[4]):
                       os.rename(local_webdav_upload_pictures + brand[4], local_webdav_upload_pictures + brand[3])
                       logger.info("file:functions.py, function:UpdateBrandFolder_local, info:folder updated from-" + str(brand[4]) + " to " + str(brand[3]))
                       cursor.commit()
                    elif not(client.check( local_webdav_upload_pictures + brand[3])):
                         os.mkdir(local_webdav_upload_pictures + brand[3])
                         cursor.commit()
                         logger.warning("file:functions.py, function:UpdateBrandFolder_local, warning:folder created intstead of updating it-" + str(brand[3]))
                 except Exception as e:
                        logger.error("file:functions.py, function:UpdateBrandFolder_local, error:" + str(e) + " | " + str(traceback))

         
    # download function 
    async def Download_files(webdav_path_download_xml, xml_download_folder_name, local_path_downloaded_xml, client, logger, webdav_https, session):
          try:
             for file in  xml_download_folder_name:
                 items = client.list(webdav_path_download_xml + file) 
                 if items:
                    for item in items:
                        try:
                           http_path = webdav_https + webdav_path_download_xml + file + "/" + item
                           local_path = os.path.join(local_path_downloaded_xml, file, item)  
                           move_from = webdav_path_download_xml + file + "/" + item
                           move_to = webdav_path_download_xml + "archive" + "/" +file + "/" + item
                           response = session.get(http_path, timeout=10)
                           if response.status_code == 200:
                              with open(local_path, "wb") as localfile:
                                   localfile.write(response.content)
                              client.move(move_from, move_to)
                              logger.info("file:functions.py, function:download_files, info:item downloaded successfully-" + str(item))
                           else:
                               logger.warning("file:functions.py, function:download_files, warning:item downloading failed-" + str(item)) 
                        except Exception as e:
                               logger.error("file:functions.py, function:download_files, error:" + str(item) + " | " + str(e) + " | " + str(traceback))    
          except Exception as e:
                 logger.error("file:functions.py, function:download_files, error:" + str(e) + " | " + str(traceback))
 
 
    # upload to remote webdav pictures
    async def Upload_picture_remote(check_folder, check_file, check_size, local_path_import_folder, cursor, logger, client, session, webdav_path_upload_picture, local_path_exported_picture, webdav_https):
          ItemPictureName_Select = Functions.ItemPictureName_Select
          ItemPicture_Insert_Get = Functions.ItemPicture_Insert_Get
          jpg_files = [filename for filename in os.listdir(local_path_import_folder) if filename.endswith(".jpg")]
          translation_table = str.maketrans('\/:*?"<>|', '---------')
          if jpg_files:
             for item in jpg_files: 
                 try:
                    picture = os.path.splitext(item)[0]
                    new_name = ItemPictureName_Select(picture, cursor, logger)
                    if (new_name and (len(new_name[1])>0)): 
                       new_name_arranged = new_name[0].translate(translation_table) # changing all forbidden caracters by '-'
                       if not(check_folder and not(client.check(webdav_path_upload_picture + new_name[1]))):
                          input_path = os.path.join(local_path_import_folder, item)
                          output_path = os.path.join(local_path_exported_picture, item)
                          remote_path = webdav_path_upload_picture + new_name[1] + "/" + new_name_arranged + ".jpg"
                          http_path = webdav_https + webdav_path_upload_picture + new_name[1] + "/" + new_name_arranged + ".jpg"
                          if not(check_file and (client.check(remote_path))): 
                             if ItemPicture_Insert_Get(picture, cursor, logger): 
                                with open(input_path, "rb") as file:                           
                                     data = file.read()
                                response = session.post(http_path, data=data, timeout=10)  
                                if response.status_code == 201 or response.status_code == 204:
                                   shutil.copy(input_path, output_path)
                                   os.remove(input_path)
                                   cursor.commit()
                                   logger.info("file:functions.py, function:Upload_picture_remote, info:pitcture item uploaded to remote webdav successfully-" + str(item))
                             else:
                                 logger.warning("file:functions.py, function:Upload_picture_remote, warning:picture item not inserted in database failed-" + str(item)) 
                          else: 
                              if check_size:
                                 local_file_size = str(os.path.getsize(input_path))
                                 webdav_file_size = client.info(remote_path)['size']  
                                 if local_file_size != webdav_file_size:
                                    with open(input_path, "rb") as file:                           
                                         data = file.read()
                                    response = session.put(http_path, data=data, timeout=10)  
                                    if response.status_code == 200:
                                       shutil.copy(input_path, output_path)
                                       os.remove(input_path)
                                       cursor.commit()
                                       logger.info("file:functions.py, function:Upload_picture_remote, info:pitcture item uploaded to remote webdav successfully-" + str(item))   
                              else:
                                  with open(input_path, "rb") as file:                           
                                       data = file.read()
                                  response = session.put(http_path, data=data, timeout=10)  
                                  if response.status_code == 200:
                                     shutil.copy(input_path, output_path)
                                     os.remove(input_path)
                                     cursor.commit()
                                     logger.info("file:functions.py, function:Upload_picture_remote, info:pitcture item uploaded successfully-" + str(item))      
                       else:
                           logger.warning("file:functions.py, function:Upload_picture_remote, warning:picture folder webdav path not exist-" + str(webdav_path_upload_picture + new_name[1])) 
                    else:
                        logger.warning("file:functions.py, function:Upload_picture_remote, warning:picture new_name or brandid is empty for item-" + str(item))    
                 except Exception as e:
                        logger.error("file:functions.py, function:Upload_picture_remote, error:" + str(item) + " | " + str(e) + " | " + str(traceback))
              
      
    # upload to remote webdav xml
    async def Upload_xml_remote(check_folder, check_file, check_size, local_path_import_folder, cursor, logger, client, session, webdav_path_upload_xml, xml_upload_folder_name, local_path_exported_xml, webdav_https):
          xml_files = [filename for filename in os.listdir(local_path_import_folder) if (filename.endswith(".xml") or filename.endswith(".csv") or filename.endswith(".xlsx"))]
          if xml_files:
             for item in xml_files: 
                 try:
                    item_lower = (os.path.splitext(item)[0]).lower()
                    for folder_name in xml_upload_folder_name:
                        if folder_name in item_lower:
                           input_path = os.path.join(local_path_import_folder, item)
                           output_path = os.path.join(local_path_exported_xml, item)  
                           remote_path= webdav_path_upload_xml + folder_name + "/" + item
                           http_path = webdav_https + webdav_path_upload_xml + folder_name + "/" + item
                           if not(check_folder and not(client.check(webdav_path_upload_xml +folder_name))):
                              if not(check_file and (client.check(remote_path))): 
                                 with open(input_path, "rb") as file:                           
                                      data = file.read()
                                 response = session.post(http_path, data=data, timeout=10)  
                                 if response.status_code == 201 or response.status_code == 204:
                                    shutil.copy(input_path, output_path)
                                    os.remove(input_path)
                                    cursor.commit()  
                                    logger.info("file:functions.py, function:Upload_xml_remote, info:xml item uploaded successfully-" + str(item))   
                                    break         
                              else:   
                                  if check_size:
                                     local_file_size = str(os.path.getsize(input_path))
                                     webdav_file_size = client.info(remote_path)['size']  
                                     if local_file_size != webdav_file_size:
                                        with open(input_path, "rb") as file:                           
                                             data = file.read()
                                        response = session.put(http_path, data=data, timeout=10)  
                                        if response.status_code == 200:
                                           shutil.copy(input_path, output_path)
                                           os.remove(input_path)
                                           cursor.commit()
                                           logger.info("file:functions.py, function:Upload_xml_remote, info:pitcture item uploaded successfully-" + str(item))   
                                           break
                                  else:
                                      with open(input_path, "rb") as file:                           
                                           data = file.read() 
                                      response = session.put(http_path, data=data, timeout=10)  
                                      if response.status_code == 200:
                                         shutil.copy(input_path, output_path)
                                         os.remove(input_path)
                                         cursor.commit()
                                         logger.info("file:functions.py, function:Upload_xml_remote, info:pitcture item uploaded successfully-" + str(item))    
                                         break                           
                           else:  
                               logger.warning("file:functions.py, function:Upload_xml_remote, warning:xml folder webdav path not exist-" + str(webdav_path_upload_xml + folder_name)) 
                               break                
                 except Exception as e:
                        logger.error("file:functions.py, function:Upload_xml_remote, error:" + str(item) + " | " + str(e) + " | " + str(traceback)) 
                                      
     
      
    # upload to local webdav pictures 
    async def Upload_picture_local(check_folder, check_file, check_size, local_webdav_upload_pictures, cursor, logger, local_path_exported_picture, local_path_import_folder):
          ItemPictureName_Select = Functions.ItemPictureName_Select
          ItemPicture_Insert_Get = Functions.ItemPicture_Insert_Get
          jpg_files = [filename for filename in os.listdir(local_path_import_folder) if filename.endswith(".jpg")]
          translation_table = str.maketrans('\/:*?"<>|', '---------')
          if jpg_files:
             for item in jpg_files: 
                 try:
                    picture_ = os.path.splitext(item)[0]
                    new_name = ItemPictureName_Select(picture_, cursor, logger)
                    if (new_name and (len(new_name[1])>0)):  
                       new_name_arranged = new_name[0].translate(translation_table) # changing all forbidden caracters by '-'
                       if not(check_folder and not(os.path.exists(local_webdav_upload_pictures + new_name[1]))):
                          input_path = os.path.join(local_path_import_folder, item)
                          output_path = os.path.join(local_path_exported_picture, item)
                          local_path = local_webdav_upload_pictures + new_name[1] + "/" + new_name_arranged + ".jpg"
                          if not(check_file and (os.path.exists(local_path))): 
                             if ItemPicture_Insert_Get(picture_, cursor, logger): 
                                shutil.copy(input_path, local_path)
                                shutil.copy(input_path, output_path)
                                os.remove(input_path)
                                cursor.commit()
                                logger.info("file:functions.py, function:Upload_picture_local, info:pitcture item uploaded to remote webdav successfully-" + str(item))
                             else:
                                 logger.warning("file:functions.py, function:Upload_picture_local, warning:picture item not inserted in database failed-" + str(item)) 
                          else: 
                              if check_size:
                                 new_local_file_size = str(os.path.getsize(input_path))
                                 local_file_size = str(os.path.getsize(local_path))
                                 if local_file_size != new_local_file_size:
                                    shutil.copy(input_path, local_path)
                                    shutil.copy(input_path, output_path)
                                    os.remove(input_path)
                                    cursor.commit()
                                    logger.info("file:functions.py, function:Upload_picture_local, info:pitcture item uploaded to remote webdav successfully-" + str(item))   
                              else:
                                  shutil.copy(input_path, local_path)
                                  shutil.copy(input_path, output_path)
                                  os.remove(input_path)
                                  cursor.commit()
                                  logger.info("file:functions.py, function:Upload_picture_local, info:pitcture item uploaded successfully-" + str(item))      
                       else:
                           logger.warning("file:functions.py, function:Upload_picture_local, warning:picture folder webdav path not exist-" + str(local_webdav_upload_pictures + new_name[1])) 
                    else:
                        logger.warning("file:functions.py, function:Upload_picture_local, warning:picture new_name or brandid is empty for item-" + str(item))    
                 except Exception as e:
                        print(e)
                        logger.error("file:functions.py, function:Upload_picture_local, error:" + str(item) + " | " + str(e) + " | " + str(traceback))
              
      
    # ulpoad to local webdav xml  
    async def Upload_xml_local(check_folder, check_file, check_size, local_webdav_upload_xml, cursor, logger, local_path_exported_xml, local_path_import_folder, xml_upload_folder_name):
          xml_files = [filename for filename in os.listdir(local_path_import_folder) if (filename.endswith(".xml") or filename.endswith(".csv") or filename.endswith(".xlsx"))]
          if xml_files:
             for item in xml_files: 
                 try:
                    item_lower = (os.path.splitext(item)[0]).lower()
                    for folder_name in xml_upload_folder_name:
                        if folder_name in item_lower:
                           input_path = os.path.join(local_path_import_folder, item)
                           output_path = os.path.join(local_path_exported_xml, item)  
                           local_path= local_webdav_upload_xml + folder_name + "/" + item
                           if not(check_folder and not(os.path.exists(local_webdav_upload_xml +folder_name))):
                              if not(check_file and (os.path.exists(local_path))):                         
                                 shutil.copy(input_path, local_path)
                                 shutil.copy(input_path, output_path)
                                 os.remove(input_path)
                                 cursor.commit()  
                                 logger.info("file:functions.py, function:Upload_xml_local, info:xml item uploaded successfully-" + str(item))            
                                 break
                              else:   
                                  if check_size:                     
                                     new_local_file_size = str(os.path.getsize(local_path))
                                     local_file_size = str(os.path.getsize(input_path))
                                     if local_file_size != new_local_file_size:
                                        shutil.copy(input_path, local_path)
                                        shutil.copy(input_path, output_path)
                                        os.remove(input_path)
                                        cursor.commit()
                                        logger.info("file:functions.py, function:Upload_xml_local, info:pitcture item uploaded successfully-" + str(item))   
                                        break
                                  else:
                                      shutil.copy(input_path, local_path)
                                      shutil.copy(input_path, output_path)
                                      os.remove(input_path)
                                      cursor.commit()
                                      logger.info("file:functions.py, function:Upload_xml_local, info:pitcture item uploaded successfully-" + str(item))         
                                      break
                           else:  
                               logger.warning("file:functions.py, function:Upload_xml_local, warning:xml folder webdav path not exist-" + str(local_webdav_upload_xml + folder_name)) 
                               break
                 except Exception as e:
                        logger.error("file:functions.py, function:Upload_xml_local, error:" + str(item) + " | " + str(e) + " | " + str(traceback))               
        
      
    # upload function (this function will upload pictures and xlss file to the cloud)
    async def Upload_files(local_path_import_folder , local_path_exported_picture, local_path_exported_xml, webdav_path_upload_xml, webdav_path_upload_picture, logger, cursor, client, xml_upload_folder_name, session, webdav_https, options, local_webdav_upload_pictures, local_webdav_upload_xml):
          try:
              Upload_picture_remote = Functions.Upload_picture_remote
              Upload_xml_remote = Functions.Upload_xml_remote
              Upload_picture_local = Functions.Upload_picture_local
              Upload_xml_local = Functions.Upload_xml_local
              CreateBrandFolder_remote = Functions.CreateBrandFolder_remote
              CreateBrandFolder_local = Functions.CreateBrandFolder_local
              UpdateBrandFolder_remote = Functions.UpdateBrandFolder_remote
              UpdateBrandFolder_local = Functions.UpdateBrandFolder_local
              check_folder = options[2]
              check_file = options[3]
              check_size = options[4]
            
              if options[0]:
                 await CreateBrandFolder_remote(client, logger, webdav_path_upload_picture, cursor) # if action = 'New
                 await UpdateBrandFolder_remote(client, logger, webdav_path_upload_picture, cursor) # if Action = 'Update'
                 await Upload_picture_remote(check_folder, check_file, check_size, local_path_import_folder, cursor, logger, client, session, webdav_path_upload_picture, local_path_exported_picture, webdav_https)
              else:
                  await (CreateBrandFolder_local(client, logger, local_webdav_upload_pictures, cursor)) # if action = 'New
                  await (UpdateBrandFolder_local(client, logger, local_webdav_upload_pictures, cursor)) # if Action = 'Update'
                  await (Upload_picture_local(check_folder, check_file, check_size, local_webdav_upload_pictures, cursor, logger, local_path_exported_picture, local_path_import_folder))
               
              if options[1]:
                 await (Upload_xml_remote(check_folder, check_file, check_size, local_path_import_folder, cursor, logger, client, session, webdav_path_upload_xml, xml_upload_folder_name, local_path_exported_xml, webdav_https))
              else:
                  await (Upload_xml_local(check_folder, check_file, check_size, local_webdav_upload_xml, cursor, logger, local_path_exported_xml, local_path_import_folder, xml_upload_folder_name))  
                    
          except Exception as e:
                 logger.error("file:functions.py, function:Upload_files, error:" + str(e) + " | " + str(traceback))
